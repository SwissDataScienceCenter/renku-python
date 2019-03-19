# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generate ASCII graph for a DAG."""

import re

import attr
import click

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

_RE_ESC = re.compile(r'\x1b[^m]*m')
"""Match escape characters."""


def _format_sha1(graph, node):
    """Return formatted text with the submodule information."""
    try:
        submodules = node.submodules

        if submodules:
            submodule = ':'.join(submodules)
            return click.style(
                submodule, fg='green'
            ) + '@' + click.style(
                node.commit.hexsha[:8], fg='yellow'
            )
    except KeyError:
        pass
    return click.style(node.commit.hexsha[:8], fg='yellow')


@attr.s
class DAG(object):
    """Generate ASCII representation of a DAG."""

    #: Symbols for nodes.
    NODE = '*'
    ROOT_NODE = '@'
    MERGE_NODE = '+'

    #: Symbols for edges.
    LEFT_EDGE = '/'
    RIGHT_EDGE = '\\'
    VERTICAL_EDGE = '|'
    HORIZONTAL_EDGE = '-'

    graph = attr.ib()
    """Define a DAG to display."""

    short = attr.ib(default=True, converter=bool)
    """Display shortened version of the DAG."""

    columns = attr.ib(default=attr.Factory(list))
    """List of columns representing nodes."""

    edges = attr.ib(default=attr.Factory(dict))
    """Map of edges."""

    last_columns_size_diff = attr.ib(default=0, converter=int)
    last_index = attr.ib(default=0, converter=int)

    def __str__(self):
        """Return string representation."""
        return '\n'.join(self)

    def __iter__(self):
        """Generate lines of ASCII representation of a DAG."""
        for node in self.graph.nodes:
            for node_symbol, lines, column_info in self.iter_edges(node):
                for line in self.iter_node_lines(
                    node_symbol, self.node_text(node), column_info
                ):
                    yield line

    def node_text(self, node):
        """Return text for a given node."""
        formatted_sha1 = _format_sha1(self.graph, node)
        path = node.path
        latest = self.graph.latest(node)

        # TODO move reference names to entity objects
        from .workflow import _deref
        refs = node.client.workflow_names[node.path]
        formatted_refs = (
            click.style(' (', fg='yellow') + ', '.join(
                click.style(_deref(name), fg='green', bold=True)
                for name in refs
            ) + click.style(')', fg='yellow')
        ) if refs else ''

        if latest:
            formatted_latest = (
                click.style(' (', fg='yellow') +
                click.style('latest -> ', fg='blue', bold=True) +
                click.style(str(latest)[:8], fg='red',
                            bold=True) + click.style(') ', fg='yellow')
            )
        else:
            formatted_latest = ' '

        result = [
            formatted_sha1 + formatted_refs + formatted_latest +
            self.graph._format_path(path)
        ]
        indentation = ' ' * len(_RE_ESC.sub('', formatted_sha1))

        # Handle subprocesses of a workflow.
        from renku.models.provenance.entities import Process

        part_of = None
        if isinstance(node, Process):
            part_of = getattr(node.activity, 'part_of', None)

        if part_of:
            step_id = node.activity._id.split('/')[-1]
            workflow_path = click.style(
                '{workflow_path}#steps/{step_id}'.format(
                    workflow_path=self.graph._format_path(part_of.path),
                    step_id=step_id,
                ),
                fg='blue',
            )

            result.append(
                '{indentation} (part of {hexsha} {workflow_path})'.format(
                    indentation=indentation,
                    hexsha=_format_sha1(self.graph, part_of),
                    workflow_path=workflow_path,
                )
            )

        parent = getattr(node, 'parent', None)

        if parent and hasattr(parent, 'members'):
            result.append(
                '{indentation} (part of {parent_path} directory)'.format(
                    indentation=indentation,
                    parent_path=click.style(
                        parent.path,
                        fg='blue',
                    ),
                )
            )

        return result

    def iter_edges(self, node):
        """Yield edges for a node and update internal status."""
        # Keep track of rendered nodes.
        key = node.commit, node.path
        if key not in self.columns:
            self.columns.append(key)

        column_index = self.columns.index(key)
        parents = [(p.commit, p.path) for p in self.graph.parents(node)]

        # Define node_symbol for node and root node.
        node_symbol = self.NODE if parents else self.ROOT_NODE

        # Split parents to ones with known edge and ones without one.
        existing_columns = []
        new_columns = []
        for parent in parents:
            assert parent != (
                node.commit, node.path
            ), 'Self reference is not allowed.'

            if parent in self.columns:
                existing_columns.append(parent)
                # Known parents already have their own edge mapping.
            else:
                new_columns.append(parent)
                self.edges[parent] = self.VERTICAL_EDGE
                # TODO shall we add a color here?

        number_of_columns = len(self.columns)
        width = 1 + number_of_columns * 2

        # Prepare columns for the next line.
        next_columns = self.columns[:]
        next_columns[column_index:column_index + 1] = new_columns
        edges = [(column_index, next_columns.index(parent))
                 for parent in existing_columns]
        self.columns[:] = next_columns

        # Insert extra lines when more than one column is added.
        while len(new_columns) > 2:
            edges.append((column_index, column_index))
            edges.append((column_index, column_index + 1))
            columns_size_diff = 1
            width += 2
            yield (
                node_symbol, width,
                (column_index, edges, number_of_columns, columns_size_diff)
            )

            # Next line will only expand edges.
            node_symbol = '\\'
            column_index += 1
            number_of_columns += 1
            edges = []
            del new_columns[0]

        # Add edges for one or two new columns.
        if len(new_columns) > 0:
            edges.append((column_index, column_index))
        if len(new_columns) > 1:
            edges.append((column_index, column_index + 1))

        # Get the final difference of columns size.
        columns_size_diff = len(next_columns) - number_of_columns
        if columns_size_diff > 0:
            width += 2

        # Remove the current node for map of edges.
        self.edges.pop(node, None)
        yield (
            node_symbol, width,
            (column_index, edges, number_of_columns, columns_size_diff)
        )

    def iter_node_lines(self, node_symbol, text, column_info):
        """Yield lines for a node of the DAG."""
        column_index, edges, number_of_columns, columns_size_diff = column_info
        # We can only add or remove one column at the time.
        assert -2 < columns_size_diff < 2, 'Too many changes on a line.'

        edge_characters = [
            # TODO shall we allow parents without edge map?
            c for p in self.columns for c in (self.edges.get(p, '|'), ' ')
        ]
        # Extend line with newly added columns.
        edge_characters.extend(
            ('|', ' ') *
            max(number_of_columns + columns_size_diff - len(self.columns), 0)
        )

        if columns_size_diff == -1:
            # Fix potentially right crossed edges into horizontal line.
            self._fix_right_crossed(edges)

        # Add a padding line for:
        add_padding_line = (
            # multi-line text when it is shrinking and
            len(text) > 2 and columns_size_diff == -1 and
            # it has a horizontal line.
            [start for (start, end) in edges if start + 1 < end]
        )

        # Fix slope of edges after the node for single line text.
        fix_tail_slope = len(text) <= 2 and not add_padding_line

        # Current line contains a NODE or ROOT_NODE character.
        current_line = edge_characters[:column_index * 2]
        # Replace the value for column index with a node symbol.
        current_line.extend([node_symbol, ' '])
        # Append fixed rest of the line from edge characters.
        current_line.extend(
            self._line_tail(
                edge_characters,
                column_index,
                number_of_columns,
                columns_size_diff,
                fix_tail_slope,
            )
        )

        # shift_line is the line containing the non-vertical
        # edges between this entry and the next
        shift_line = edge_characters[:column_index * 2]
        for i in range(2 + columns_size_diff):
            shift_line.append(' ')
        count = number_of_columns - column_index - 1
        if columns_size_diff == -1:
            for i in range(count):
                shift_line.extend([self.LEFT_EDGE, ' '])
        elif columns_size_diff == 0:
            shift_line.extend(
                edge_characters[(column_index + 1) * 2:number_of_columns * 2]
            )
        else:
            for i in range(count):
                shift_line.extend([self.RIGHT_EDGE, ' '])

        # Render edges from the current node to its parents.
        self._render_edges(edge_characters, edges, current_line, shift_line)

        # All graph lines that should be printed for this node.
        lines = [current_line]
        if add_padding_line:  # ... for long text.
            lines.append(
                self._padding_line(
                    edge_characters, column_index, number_of_columns, edges
                )
            )

        if self.short:
            shift_chars = {self.LEFT_EDGE, self.RIGHT_EDGE}
            if any(c in shift_chars for c in shift_line if c):
                # Contains edge shifts so add it.
                lines.append(shift_line)
        else:
            lines.append(shift_line)

        # Add missing graph lines for long log strings.
        extra_shift_line = edge_characters[:(
            number_of_columns + columns_size_diff
        ) * 2]
        if len(lines) < len(text):
            while len(lines) < len(text):
                lines.append(extra_shift_line[:])

        # Keep the text is left aligned when more columns follow.
        indentation_level = number_of_columns + max(columns_size_diff, 0)

        for (line, msg) in zip_longest(lines, text, fillvalue=''):
            if node_symbol in {self.ROOT_NODE, self.NODE}:
                ln = '%-*s %s' % (
                    2 * indentation_level, ''.join(line).rstrip(), msg
                )
            else:
                ln = '%-*s' % (2 * indentation_level, ''.join(line))

            yield ln.rstrip()

        if not self.edges:
            yield ''  # TODO Show independent file history.

        # Update the column index and size change.
        self.last_index = column_index
        self.last_columns_size_diff = columns_size_diff

    @staticmethod
    def _fix_right_crossed(edges):
        """Change right crossed edges to a horizontal bar.

        :param edges: List of edges is **mutated** when a problem is found.
        """
        for index, (start, end) in enumerate(edges):
            if end > start:
                edges[index] = (start, end + 1)

    def _line_tail(
        self, edge_characters, column_index, number_of_columns,
        columns_size_diff, fix_tail
    ):
        """Return a line tail from the given column index."""
        if fix_tail and columns_size_diff == self.last_columns_size_diff \
                and columns_size_diff != 0:
            # Fix the edge orientation for non-vertical direction.
            if columns_size_diff == -1:
                start = max(column_index + 1, self.last_index)
                tail = edge_characters[column_index * 2:(start - 1) * 2]
                # Extend line with correctly oriented edges.
                tail.extend(
                    [self.LEFT_EDGE, ' '] * (number_of_columns - start)
                )
                return tail
            else:
                # Next line has more columns, hence the edge orientation.
                return [self.RIGHT_EDGE, ' '
                        ] * (number_of_columns - column_index - 1)
        else:
            # No fix required, just copy the rest.
            remainder = (number_of_columns - column_index - 1)
            return edge_characters[-(remainder * 2):] if remainder > 0 else []

    def _render_edges(self, edge_characters, edges, current_line, shift_line):
        """Render edges from the current node to its parents.

        :param edge_characters: List with characters representing edges.
        :param edges: List of edges.
        :param current_line: **Mutated** list of characters for current line.
        :param shift_line: **Mutated** list of characters for shifting line.
        """
        for (start, end) in edges:
            if start == end + 1:
                # Shift to the left.
                shift_line[2 * end + 1] = self.LEFT_EDGE
            elif start == end - 1:
                # Extend to the right.
                shift_line[2 * start + 1] = self.RIGHT_EDGE
            elif start == end:
                # Copy the previous character.
                shift_line[2 * start] = edge_characters[2 * start]
            else:
                if 2 * end >= len(current_line):
                    continue
                # Render a horizontal merge line.
                current_line[2 * end] = self.MERGE_NODE
                if start > end:
                    (start, end) = (end, start)
                for i in range(2 * start + 1, 2 * end):
                    if current_line[i] != self.MERGE_NODE:
                        current_line[i] = self.HORIZONTAL_EDGE

    @staticmethod
    def _padding_line(edge_characters, column_index, number_of_columns, edges):
        """Return a padding line generated from the current edges."""
        # Reuse everything up to the current node.
        line = edge_characters[:column_index * 2]
        if (column_index, column_index - 1) in edges or \
                (column_index, column_index) in edges:
            # Copy the edge from the current node.
            line.extend(
                edge_characters[column_index * 2:(column_index + 1) * 2]
            )
        else:
            # There is no vertical branch from the current node.
            line.extend([' ', ' '])

        # Copy edges from the right of the current node.
        remainder = number_of_columns - column_index - 1
        if remainder > 0:
            line.extend(edge_characters[-(remainder * 2):])
        return line
