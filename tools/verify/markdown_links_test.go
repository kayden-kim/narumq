package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyMarkdownLinksResolvesLocalTargetsAndHeadingAnchors(t *testing.T) {
	root := t.TempDir()
	writeMarkdownLinkFixture(t, root, "AGENTS.md", "# Repository rules\n")
	writeMarkdownLinkFixture(t, root, "README.md", "# Repository overview\n")
	writeMarkdownLinkFixture(t, root, "docs/source.md", `---
example: "[front matter](missing-front-matter.md)"
---
# Source

[heading](target.md#stable-state)
[duplicate heading](target.md#repeat-1)
[explicit id](target.md#explicit-anchor)
[directory heading](assets/#asset-title)
[self](#source)
[external](https://example.com/page)
[network path](//example.com/page)

`+"```md\n[code fence](missing-code-fence.md)\n```\n")
	writeMarkdownLinkFixture(t, root, "docs/target.md", "# Stable State\n\n## Repeat\n\n## Repeat\n\n<div id=\"explicit-anchor\"></div>\n")
	writeMarkdownLinkFixture(t, root, "docs/assets/README.md", "# Asset title\n")
	writeMarkdownLinkFixture(t, root, ".agents/skills/example/SKILL.md", "# Skill\n")
	writeMarkdownLinkFixture(t, root, "outside/ignored.md", "[ignored](missing.md)\n")

	summary, err := verifyMarkdownLinks(root)
	if err != nil {
		t.Fatalf("verifyMarkdownLinks() error = %v", err)
	}
	if summary.MarkdownFiles != 6 {
		t.Fatalf("MarkdownFiles = %d, want 6", summary.MarkdownFiles)
	}
	if summary.LocalLinks != 5 {
		t.Fatalf("LocalLinks = %d, want 5", summary.LocalLinks)
	}
}

func TestVerifyMarkdownLinksReportsMissingTargetAndHeading(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".agents", "skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeMarkdownLinkFixture(t, root, "AGENTS.md", "# Repository rules\n")
	writeMarkdownLinkFixture(t, root, "README.md", "# Repository overview\n")
	writeMarkdownLinkFixture(t, root, "docs/source.md", "[missing target](missing.md)\n[missing heading](target.md#not-here)\n")
	writeMarkdownLinkFixture(t, root, "docs/target.md", "# Present\n")

	summary, err := verifyMarkdownLinks(root)
	if err == nil {
		t.Fatal("verifyMarkdownLinks() error = nil, want broken-link error")
	}
	if summary.MarkdownFiles != 4 || summary.LocalLinks != 2 {
		t.Fatalf("summary = %+v, want 4 Markdown files and 2 local links", summary)
	}
	for _, want := range []string{
		"found 2 invalid link(s) in 4 Markdown files (2 local links checked)",
		`docs/source.md: local link "missing.md": target does not exist`,
		`docs/source.md: local link "target.md#not-here": heading #not-here does not exist in docs/target.md`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func writeMarkdownLinkFixture(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
