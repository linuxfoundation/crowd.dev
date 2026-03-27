import os
import re

import tomllib
import yaml


class SectionExtractor:
    """
    Extracts relevant sections from file content based on file format and governance keywords.
    Returns None when no relevant sections are found or format is unsupported (caller falls back to full content).
    """

    def extract(self, filename: str, content: str, keywords: set[str]) -> str | None:
        """
        Returns extracted relevant section text, or None if no sections found.
        filename should be the basename (lowercased).
        """
        ext = os.path.splitext(filename)[1]
        if ext in (".md", ".markdown"):
            return self._extract_markdown_sections(content, keywords)
        elif ext in (".yaml", ".yml"):
            return self._extract_yaml_sections(content, keywords)
        elif ext == ".toml":
            return self._extract_toml_sections(content, keywords)
        return None

    def _extract_markdown_sections(self, content: str, keywords: set[str]) -> str | None:
        """
        Splits content on `#`-style heading lines only (# / ## / ###...).
        Includes a section if its heading text contains any keyword.
        Returns joined matching sections, or None if none match.
        """
        heading_pattern = re.compile(r"^#{1,6}\s+", re.MULTILINE)
        # Split into (heading_line, body) pairs; first element may be pre-heading content
        parts = heading_pattern.split(content)
        headings = heading_pattern.findall(content)

        # parts[0] is text before the first heading (skip it)
        # parts[1..] correspond to headings[0..]
        matching_sections = []
        for i, heading_marker in enumerate(headings):
            block = parts[i + 1]  # block starts right after the heading marker
            # The first line of block is the heading text
            first_newline = block.find("\n")
            heading_text = block[:first_newline].strip() if first_newline != -1 else block.strip()
            if any(kw in heading_text.lower() for kw in keywords):
                matching_sections.append(f"{heading_marker}{block}")

        return "".join(matching_sections) if matching_sections else None

    def _extract_yaml_sections(self, content: str, keywords: set[str]) -> str | None:
        """
        Parses YAML and returns top-level keys whose name contains any keyword, serialized back to YAML.
        Returns None if no keys match or parsing fails.
        """
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError:
            return None

        if not isinstance(data, dict):
            return None

        matching = {k: v for k, v in data.items() if any(kw in str(k).lower() for kw in keywords)}
        if not matching:
            return None

        return yaml.dump(matching, default_flow_style=False, allow_unicode=True)

    def _extract_toml_sections(self, content: str, keywords: set[str]) -> str | None:
        """
        Parses TOML and returns top-level keys whose name contains any keyword,
        serialized as Python repr key=value lines (not valid TOML syntax).
        Returns None if no keys match or parsing fails.
        """
        try:
            data = tomllib.loads(content)
        except tomllib.TOMLDecodeError:
            return None

        matching = {k: v for k, v in data.items() if any(kw in k.lower() for kw in keywords)}
        if not matching:
            return None

        # Serialize matching keys back as simple TOML representation
        lines = []
        for k, v in matching.items():
            lines.append(f"{k} = {repr(v)}")
        return "\n".join(lines)
