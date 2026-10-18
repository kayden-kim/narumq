package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

const (
	jwtIssuer      = "https://issuer.example"
	jwtAudience    = "narumq"
	jwtKeyID       = "capacity-key"
	fixtureTTL     = 24 * time.Hour
	jwksFileName   = "jwks.json"
	jwksListenPath = "/jwks.json"
)

type securityConfigDoc struct {
	JWT        jwtConfigDoc  `json:"jwt"`
	Identities []identityDoc `json:"identities"`
	ACL        []aclRuleDoc  `json:"acl"`
}

type jwtConfigDoc struct {
	Issuer                string   `json:"issuer"`
	Audience              string   `json:"audience"`
	Algorithms            []string `json:"algorithms"`
	JWKSURL               string   `json:"jwksUrl"`
	CacheTTLMillis        uint32   `json:"cacheTtlMillis"`
	RefreshCooldownMillis uint32   `json:"refreshCooldownMillis"`
	ClockSkewMillis       uint32   `json:"clockSkewMillis"`
	HTTPTimeoutMillis     uint32   `json:"httpTimeoutMillis"`
}

type identityDoc struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type aclRuleDoc struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

// buildSecurityConfig returns the broker security document for every cell the profile can
// reach: one JWT identity per tenant, exact client ID lists, and per-tenant topic ACLs.
func buildSecurityConfig(p Profile, jwksURL string) securityConfigDoc {
	doc := securityConfigDoc{JWT: jwtConfigDoc{
		Issuer: jwtIssuer, Audience: jwtAudience, Algorithms: []string{"RS256"}, JWKSURL: jwksURL,
		CacheTTLMillis: 300000, RefreshCooldownMillis: 30000, ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
	}}
	for cell := range p.MaxCells() {
		for _, kind := range []byte{'a', 'b'} {
			tenant := tenantID(kind, cell)
			filter := topicRoot(kind, cell) + "/#"
			doc.Identities = append(doc.Identities, identityDoc{
				Issuer: jwtIssuer, Subject: tenant, TenantID: tenant, CredentialID: tenant + "-credential",
				AllowedClientIDs: tenantClientIDs(p, kind, cell), Roles: []string{"client"},
			})
			doc.ACL = append(doc.ACL,
				aclRuleDoc{TenantID: tenant, Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: filter},
				aclRuleDoc{TenantID: tenant, Role: "client", Effect: "allow", Action: "publish", TopicFilter: filter},
			)
		}
	}
	return doc
}

type fixtureCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
}

func randomSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
}

func newFixtureCA(validity time.Duration) (*fixtureCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ capacity CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(validity), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &fixtureCA{certificate: certificate, key: key}, nil
}

func (ca *fixtureCA) pem() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
}

type issuedCertificate struct {
	certificatePEM []byte
	keyPEM         []byte
	fingerprint    string
}

// issue creates a leaf certificate whose SANs are exactly dnsNames plus localhost.
func (ca *fixtureCA) issue(validity time.Duration, dnsNames []string, usages ...x509.ExtKeyUsage) (issuedCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return issuedCertificate{}, err
	}
	serial, err := randomSerial()
	if err != nil {
		return issuedCertificate{}, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: dnsNames[0]},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(validity),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages,
		DNSNames: append(append([]string(nil), dnsNames...), "localhost"), BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		return issuedCertificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return issuedCertificate{}, err
	}
	sum := sha256.Sum256(der)
	return issuedCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:         pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(sum[:]),
	}, nil
}

// jwtSigner mints per-tenant tokens; the private key lives only in this process.
type jwtSigner struct {
	key      jwk.Key
	jwksJSON []byte
}

func newJWTSigner() (*jwtSigner, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(signingKey.Set("kid", jwtKeyID), signingKey.Set("alg", "RS256")); err != nil {
		return nil, err
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, err
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, err
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, err
	}
	return &jwtSigner{key: signingKey, jwksJSON: jwksJSON}, nil
}

func (s *jwtSigner) mint(tenant string, validity time.Duration) (string, error) {
	token := jwt.New()
	token.Options().Enable(jwt.FlattenAudience)
	for name, value := range map[string]any{
		"iss": jwtIssuer, "aud": []string{jwtAudience}, "sub": tenant, "exp": time.Now().Add(validity),
	} {
		if err := token.Set(name, value); err != nil {
			return "", err
		}
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), s.key))
	if err != nil {
		return "", err
	}
	return string(signed), nil
}

// mintCredentials returns {tenantId: {username, token}} for every tenant the profile can reach.
func (s *jwtSigner) mintCredentials(p Profile, validity time.Duration) (map[string]credential, error) {
	creds := make(map[string]credential, p.MaxCells()*2)
	for cell := range p.MaxCells() {
		for _, kind := range []byte{'a', 'b'} {
			tenant := tenantID(kind, cell)
			token, err := s.mint(tenant, validity)
			if err != nil {
				return nil, err
			}
			creds[tenant] = credential{Username: tenant, Token: token}
		}
	}
	return creds, nil
}

func writeSecretFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}

func loadProfileFlag(name string, args []string, extra func(*flag.FlagSet)) (Profile, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	profilePath := flags.String("profile", "", "capacity profile JSON")
	if extra != nil {
		extra(flags)
	}
	if err := flags.Parse(args); err != nil {
		return Profile{}, err
	}
	if *profilePath == "" {
		return Profile{}, errors.New("--profile is required")
	}
	p, _, _, err := LoadProfile(*profilePath)
	return p, err
}

// serveJWKSCommand serves one JWKS file at /jwks.json for the broker's JWT verifier.
func serveJWKSCommand(ctx context.Context, args []string) error {
	var file, listen string
	flags := flag.NewFlagSet("serve-jwks", flag.ContinueOnError)
	flags.StringVar(&file, "file", "", "JWKS file to serve")
	flags.StringVar(&listen, "listen", "127.0.0.1:18080", "listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if file == "" {
		return errors.New("--file is required")
	}
	if _, err := os.Stat(file); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc(jwksListenPath, func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(file)
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	})
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// securityConfigCommand writes the broker security.json for a profile.
func securityConfigCommand(args []string) error {
	var jwksURL, out string
	p, err := loadProfileFlag("security-config", args, func(flags *flag.FlagSet) {
		flags.StringVar(&jwksURL, "jwks-url", "", "JWKS URL (https or loopback http)")
		flags.StringVar(&out, "out", "", "output security.json path")
	})
	if err != nil {
		return err
	}
	if jwksURL == "" || out == "" {
		return errors.New("--jwks-url and --out are required")
	}
	data, err := json.Marshal(buildSecurityConfig(p, jwksURL))
	if err != nil {
		return err
	}
	return writeSecretFile(out, data)
}

// fixtureCommand writes EKS runner fixtures: public JWKS, per-tenant tokens, CA and a server
// certificate. The signing key and CA key are discarded when the process exits.
func fixtureCommand(args []string) error {
	var out, serverName string
	p, err := loadProfileFlag("fixture", args, func(flags *flag.FlagSet) {
		flags.StringVar(&out, "out", "", "output directory")
		flags.StringVar(&serverName, "tls-server-name", "", "server certificate SAN")
	})
	if err != nil {
		return err
	}
	if out == "" || serverName == "" {
		return errors.New("--out and --tls-server-name are required")
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	signer, err := newJWTSigner()
	if err != nil {
		return err
	}
	creds, err := signer.mintCredentials(p, fixtureTTL)
	if err != nil {
		return err
	}
	credsJSON, err := json.Marshal(creds)
	if err != nil {
		return err
	}
	ca, err := newFixtureCA(fixtureTTL)
	if err != nil {
		return err
	}
	server, err := ca.issue(fixtureTTL, []string{serverName}, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return err
	}
	files := map[string][]byte{
		jwksFileName: signer.jwksJSON, "credentials.json": credsJSON,
		"ca.pem": ca.pem(), "server.crt": server.certificatePEM, "server.key": server.keyPEM,
	}
	names := make([]string, 0, len(files))
	for name, data := range files {
		if err := writeSecretFile(filepath.Join(out, name), data); err != nil {
			return err
		}
		names = append(names, name)
	}
	summary, err := json.Marshal(map[string]any{
		"tenants": len(creds), "clients": p.MaxCells() * cellClients, "files": names,
	})
	if err != nil {
		return err
	}
	fmt.Println(string(summary))
	return nil
}
