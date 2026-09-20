package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type certPair struct {
	certPEM []byte
	keyPEM  []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

func generateCA(t *testing.T, commonName string) certPair {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA private key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate CA serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}

	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal CA key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPair{
		certPEM: certPEM,
		keyPEM:  keyPEM,
		cert:    parsed,
		key:     priv,
	}
}

func generateServerCert(t *testing.T, ca certPair, commonName string, hosts []string) certPair {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server private key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate server serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Test"},
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}

	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("parse server certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPair{
		certPEM: certPEM,
		keyPEM:  keyPEM,
		cert:    parsed,
		key:     priv,
	}
}

func generateClientCert(t *testing.T, ca certPair, commonName string, dnsNames []string, emails []string) certPair {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client private key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate client serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Test"},
		},
		NotBefore:      time.Now().Add(-1 * time.Hour),
		NotAfter:       time.Now().Add(24 * time.Hour),
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:       dnsNames,
		EmailAddresses: emails,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}

	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("parse client certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPair{
		certPEM: certPEM,
		keyPEM:  keyPEM,
		cert:    parsed,
		key:     priv,
	}
}

func generateExpiredClientCert(t *testing.T, ca certPair, commonName string) certPair {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client private key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate client serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"NaruMQ Test"},
		},
		NotBefore:   time.Now().Add(-48 * time.Hour),
		NotAfter:    time.Now().Add(-24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create expired client certificate: %v", err)
	}

	parsed, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("parse expired client certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPair{
		certPEM: certPEM,
		keyPEM:  keyPEM,
		cert:    parsed,
		key:     priv,
	}
}

func writeTempFile(t *testing.T, dir, filename string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write temp file %s: %v", path, err)
	}
	return path
}
