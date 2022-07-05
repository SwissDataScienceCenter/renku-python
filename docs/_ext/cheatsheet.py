import json
from collections import defaultdict

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from sphinx.locale import _
from sphinx.util import texescape
from sphinx.util.docutils import SphinxDirective


def latex_escape(text):
    """Escape characters in text for use in latex."""
    return texescape.escape(text).replace(r"\sphinxhyphen{}", "-")


def add_linebreaks(text, breakstring=" \\linebreak "):
    """Replace ||| with linebreaks."""
    return text.replace("\n", " ").replace("|||", breakstring)


class cheatsheet_list(nodes.General, nodes.Element):
    """Sphinx element to output a list of cheatsheet entries."""

    pass


class CheatsheetListDirective(Directive):
    """Sphinx directive to output a list of cheatsheet elements."""

    def run(self):
        """Run method that produces dummy node to be replaced with cheatsheet in post processing."""
        return [cheatsheet_list("")]


class CheatsheetDirective(SphinxDirective):
    """Sphinx directive to define a cheatsheet entry."""

    has_content = True

    option_spec = {
        "command": directives.unchanged_required,
        "description": directives.unchanged_required,
        "group": directives.unchanged_required,
        "extended": directives.flag,
    }

    def run(self):
        """Process a cheatsheet entry."""

        if not hasattr(self.env, "cheatsheet_all_entries"):
            self.env.cheatsheet_all_entries = []

        command = self.options.get("command")
        description = self.options.get("description")
        group = self.options.get("group")

        if any(
            command == e["command"] and description == e["description"] and group == e["group"]
            for e in self.env.cheatsheet_all_entries
        ):
            return []

        self.env.cheatsheet_all_entries.append(
            {
                "command": command,
                "description": description,
                "group": group,
                "extended": True if "extended" in self.options else False,
                "docname": self.env.docname,
            }
        )

        return []


def purge_cheatsheet(app, env, docname):
    """Clear environment if source files changed."""
    if not hasattr(env, "cheatsheet_all_entries"):
        return

    env.cheatsheet_all_entries = [entry for entry in env.cheatsheet_all_entries if entry["docname"] != docname]


def merge_cheatsheets(app, env, docnames, other):
    """Merge cheatsheet entries during parallel build."""
    if not hasattr(env, "cheatsheet_all_entries"):
        env.cheatsheet_all_entries = []

    if hasattr(other, "cheatsheet_all_entries"):
        env.cheatsheet_all_entries.extend(other.cheatsheet_all_entries)


def process_latex_entries(content, entries, groups):
    """Create output when building latex cheatsheet."""
    for group in groups:
        entry_list = entries[group]
        content.append(nodes.raw("", f"\section{{{group}}}", format="latex"))

        for entry in entry_list:
            description = latex_escape(entry["description"])
            description = add_linebreaks(description)
            command = latex_escape(entry["command"])
            command = add_linebreaks(command)
            content.append(nodes.raw("", f"\commandsubsection{{{command}}}{{{description}}}", format="latex"))


def process_json_entries(content, entries, groups):
    """Create output when building json cheatsheet."""
    data = {"groups": []}
    for group in groups:
        entry_list = entries[group]
        group_entry = {"name": group, "commands": []}

        for entry in entry_list:
            group_entry["commands"].append(
                {"command": entry["command"], "description": entry["description"], "extended": entry["extended"]}
            )

        data["groups"].append(group_entry)

    content.append(nodes.raw("", data, format="html"))


def process_regular_entries(content, entries, groups):
    """Create output when building regular (html) cheatsheet."""
    for group in groups:
        entry_list = entries[group]
        paragraph = nodes.section(ids=[group])
        paragraph += nodes.title(group, group)

        for entry in entry_list:
            description = entry["description"]
            command = entry["command"]

            entry_paragraph = nodes.paragraph()

            command = "<p class='cheatsheet_command'>" + add_linebreaks(command, "<br />") + "</p>"
            command = nodes.raw("", command, format="html")
            entry_paragraph += command

            description = "<p class='cheatsheet_description'>" + add_linebreaks(description, "<br />") + "</p>"
            description = nodes.raw("", description, format="html")
            entry_paragraph += description

            paragraph += entry_paragraph

        content.append(paragraph)


def process_cheatsheet_nodes(app, doctree, fromdocname):
    """Process cached cheatsheet entries to create a cheatsheet list."""

    env = app.builder.env
    if not hasattr(env, "cheatsheet_all_entries"):
        env.cheatsheet_all_entries = []

    for node in doctree.traverse(cheatsheet_list):
        content = []

        entries = defaultdict(list)

        for cheatsheet_info in env.cheatsheet_all_entries:
            if not cheatsheet_info["extended"] or app.config.cheatsheet_extended:
                entries[cheatsheet_info["group"]].append(cheatsheet_info)

        if app.builder.name == "latex":
            process_latex_entries(content, entries, app.config.cheatsheet_groups)
        elif app.builder.name == "json":
            process_json_entries(content, entries, app.config.cheatsheet_groups)
        else:
            process_regular_entries(content, entries, app.config.cheatsheet_groups)

        node.replace_self(content)


def setup(app):
    """Run setup method for directive/plugin."""

    app.add_config_value("cheatsheet_extended", False, "html")
    app.add_config_value("cheatsheet_groups", [], "html")
    app.add_node(cheatsheet_list)

    app.add_directive("cheatsheet", CheatsheetDirective)
    app.add_directive("cheatsheetlist", CheatsheetListDirective)
    app.connect("doctree-resolved", process_cheatsheet_nodes)
    app.connect("env-purge-doc", purge_cheatsheet)
    app.connect("env-merge-info", merge_cheatsheets)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
