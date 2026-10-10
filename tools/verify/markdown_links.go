package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

var (
	frontMatterPattern       = regexp.MustCompile(`(?s)\A---\r?\n.*?\r?\n---[ \t]*\r?\n`)
	fencedCodePattern        = regexp.MustCompile("(?m)^[ \\t]*```[^\\n]*\\r?\\n[\\s\\S]*?^[ \\t]*```[ \\t]*\\r?$")
	inlineLinkPattern        = regexp.MustCompile(`\]\(\s*(<[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)`)
	externalURLPattern       = regexp.MustCompile(`(?i)^[a-z][a-z0-9+.-]*:`)
	markdownHeadingPattern   = regexp.MustCompile(`(?m)^[ \t]{0,3}#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$`)
	headingLinkPattern       = regexp.MustCompile(`\[([^\]]+)\]\([^)]*\)`)
	headingHTMLPattern       = regexp.MustCompile(`<[^>]*>`)
	headingIDPattern         = regexp.MustCompile(`(?i)<[^>]+\b(?:id|name)=["']([^"']+)["'][^>]*>`)
	explicitHeadingIDPattern = regexp.MustCompile(`[ \t]+\{#([^}]+)\}[ \t]*$`)
)

type markdownLinkSummary struct {
	MarkdownFiles int
	LocalLinks    int
}

type markdownDocument struct {
	content string
	anchors map[string]struct{}
}

func verifyMarkdownLinks(root string) (markdownLinkSummary, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return markdownLinkSummary{}, fmt.Errorf("resolve repository root: %w", err)
	}

	files, err := markdownFiles(root)
	if err != nil {
		return markdownLinkSummary{}, err
	}

	documents := make(map[string]markdownDocument, len(files))
	for _, path := range files {
		content, err := os.ReadFile(path)
		if err != nil {
			return markdownLinkSummary{}, fmt.Errorf("read Markdown file %s: %w", markdownDisplayPath(root, path), err)
		}
		body := markdownBody(string(content))
		documents[path] = markdownDocument{content: body, anchors: markdownAnchors(body)}
	}

	summary := markdownLinkSummary{MarkdownFiles: len(files)}
	var issues []string
	for _, source := range files {
		document := documents[source]
		matches := inlineLinkPattern.FindAllStringSubmatchIndex(document.content, -1)
		for _, match := range matches {
			href := document.content[match[2]:match[3]]
			href = strings.TrimPrefix(strings.TrimSuffix(href, ">"), "<")
			if href == "" || externalURLPattern.MatchString(href) || strings.HasPrefix(href, "//") {
				continue
			}
			summary.LocalLinks++

			localPath, fragment := splitMarkdownHref(href)
			target := filepath.Clean(filepath.Join(filepath.Dir(source), filepath.FromSlash(localPath)))
			if localPath == "" {
				target = source
			}
			info, err := os.Stat(target)
			if err != nil {
				issues = append(issues, fmt.Sprintf("%s: local link %q: target does not exist", markdownDisplayPath(root, source), href))
				continue
			}

			anchorTarget := target
			if info.IsDir() {
				if fragment == "" {
					continue
				}
				anchorTarget = filepath.Join(target, "README.md")
				if _, err := os.Stat(anchorTarget); err != nil {
					issues = append(issues, fmt.Sprintf("%s: local link %q: directory target has no README.md for fragment", markdownDisplayPath(root, source), href))
					continue
				}
			}
			if fragment == "" || !isMarkdownFile(anchorTarget) {
				continue
			}
			anchors, err := markdownTargetAnchors(root, anchorTarget, documents)
			if err != nil {
				issues = append(issues, fmt.Sprintf("%s: local link %q: read target: %v", markdownDisplayPath(root, source), href, err))
				continue
			}
			if _, ok := anchors[fragment]; !ok {
				issues = append(issues, fmt.Sprintf("%s: local link %q: heading #%s does not exist in %s", markdownDisplayPath(root, source), href, fragment, markdownDisplayPath(root, anchorTarget)))
			}
		}
	}

	if len(issues) > 0 {
		sort.Strings(issues)
		return summary, fmt.Errorf("found %d invalid link(s) in %d Markdown files (%d local links checked):\n%s", len(issues), summary.MarkdownFiles, summary.LocalLinks, strings.Join(issues, "\n"))
	}
	return summary, nil
}

func markdownFiles(root string) ([]string, error) {
	files := make([]string, 0)
	for _, name := range []string{"AGENTS.md", "README.md"} {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat Markdown file %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("Markdown path %s is not a regular file", name)
		}
		files = append(files, path)
	}

	for _, directory := range []string{filepath.Join(root, "docs"), filepath.Join(root, ".agents", "skills")} {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !entry.Type().IsRegular() || !isMarkdownFile(path) {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk Markdown directory %s: %w", markdownDisplayPath(root, directory), err)
		}
	}
	sort.Strings(files)
	return files, nil
}

func markdownBody(content string) string {
	content = frontMatterPattern.ReplaceAllString(content, "")
	return fencedCodePattern.ReplaceAllString(content, "")
}

func markdownAnchors(content string) map[string]struct{} {
	anchors := make(map[string]struct{})
	for _, match := range headingIDPattern.FindAllStringSubmatch(content, -1) {
		anchors[match[1]] = struct{}{}
	}

	duplicates := make(map[string]int)
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSuffix(line, "\r")
		match := markdownHeadingPattern.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		title := match[1]
		if explicit := explicitHeadingIDPattern.FindStringSubmatch(title); len(explicit) == 2 {
			anchors[explicit[1]] = struct{}{}
			title = explicitHeadingIDPattern.ReplaceAllString(title, "")
		}
		title = headingLinkPattern.ReplaceAllString(title, "$1")
		title = headingHTMLPattern.ReplaceAllString(title, "")
		title = strings.NewReplacer("`", "", "*", "", "_", "", "~", "").Replace(title)
		slug := markdownHeadingSlug(title)
		if slug == "" {
			continue
		}
		duplicate := duplicates[slug]
		if duplicate > 0 {
			anchors[fmt.Sprintf("%s-%d", slug, duplicate)] = struct{}{}
		} else {
			anchors[slug] = struct{}{}
		}
		duplicates[slug] = duplicate + 1
	}
	return anchors
}

func markdownHeadingSlug(title string) string {
	var cleaned strings.Builder
	for _, character := range strings.ToLower(title) {
		if unicode.IsLetter(character) || unicode.IsNumber(character) || unicode.IsSpace(character) || character == '-' {
			cleaned.WriteRune(character)
		}
	}
	return strings.Join(strings.Fields(cleaned.String()), "-")
}

func splitMarkdownHref(href string) (string, string) {
	if decoded, err := url.PathUnescape(href); err == nil {
		href = decoded
	}
	hash := strings.IndexByte(href, '#')
	query := strings.IndexByte(href, '?')
	localPath := href
	if hash >= 0 {
		localPath = href[:hash]
	}
	if query >= 0 && (hash < 0 || query < hash) {
		localPath = href[:query]
	}
	if hash < 0 {
		return localPath, ""
	}
	fragment := href[hash+1:]
	if queryInFragment := strings.IndexByte(fragment, '?'); queryInFragment >= 0 {
		fragment = fragment[:queryInFragment]
	}
	return localPath, fragment
}

func markdownTargetAnchors(root, path string, documents map[string]markdownDocument) (map[string]struct{}, error) {
	path = filepath.Clean(path)
	if document, ok := documents[path]; ok {
		return document.anchors, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", markdownDisplayPath(root, path), err)
	}
	return markdownAnchors(markdownBody(string(content))), nil
}

func markdownDisplayPath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}

func isMarkdownFile(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".md")
}
