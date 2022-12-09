# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Curses utilities."""
import curses
import curses.panel
from typing import Dict, List, Optional, Tuple

from renku.command.view_model.text_canvas import Point
from renku.core import errors
from renku.domain_model.provenance.activity import Activity


class CursesActivityGraphViewer:
    """An interactive viewer for activity graphs."""

    COLOR_MAPPING = {
        "[31": curses.COLOR_RED,
        "[32": curses.COLOR_GREEN,
        "[33": curses.COLOR_YELLOW,
        "[34": curses.COLOR_BLUE,
        "[35": curses.COLOR_MAGENTA,
        "[36": curses.COLOR_CYAN,
        "[37": curses.COLOR_WHITE,
        "[0": -1,
    }

    ACTIVITY_OVERLAY_WIDTH = 60
    ACTIVITY_OVERLAY_HEIGHT = 40
    HELP_OVERLAY_WIDTH = 60
    HELP_OVERLAY_HEIGHT = 6
    DATE_FORMAT = "%Y-%m-%d %H:%M:S"

    def __init__(
        self,
        text_data: str,
        navigation_data: List[List[Tuple[Point, Point, Activity]]],
        vertical_space: int,
        use_color: bool,
    ):
        self.text_data = text_data
        self.navigation_data = navigation_data
        self.vertical_space = vertical_space

        self.current_layer = 0
        self.layer_position = 0
        self.max_layer = len(navigation_data) - 1
        self.y_pos = 0
        self.x_pos = 0
        self.color_cache: Dict[str, int] = {}
        self.activity_overlay: Optional[curses._CursesWindow] = None
        self.help_overlay: Optional[curses._CursesWindow] = None
        self._select_activity()
        self.use_color = use_color

    def _init_curses(self, screen):
        """Initialize curses screen for interactive mode."""
        screen.keypad(True)
        curses.use_default_colors()
        curses.start_color()
        curses.curs_set(0)
        curses.noecho()
        screen.refresh()
        self._init_console_colors()
        self.rows, self.cols = screen.getmaxyx()

        min_width = max(self.ACTIVITY_OVERLAY_WIDTH, self.HELP_OVERLAY_WIDTH) + 1
        min_height = max(self.ACTIVITY_OVERLAY_HEIGHT, self.HELP_OVERLAY_HEIGHT) + 1
        if self.cols < min_width or self.rows < min_height:
            raise errors.TerminalSizeError(
                f"Terminal is too small for interactive mode, size should be at least {min_width}x{min_height}."
            )

        text_data_lines = self.text_data.splitlines()

        self.content_max_x = max(len(line) for line in text_data_lines)
        self.content_max_y = len(self.text_data)
        self.content_pad = curses.newpad(self.content_max_y, self.content_max_x)
        for i, l in enumerate(text_data_lines):
            self._addstr_with_color_codes(self.content_pad, i, 0, l)

        self._blink_text(self.content_pad, self.activity_start, self.activity_end, bold=True)

        self._update_help_overlay(screen)

        self._refresh(screen)

    def _init_console_colors(self):
        """Setup curses color mapping."""
        if not self.use_color:
            curses.init_pair(100, -1, -1)
            for color_symbol, _ in self.COLOR_MAPPING.items():
                self.color_cache[color_symbol] = 100
        else:
            for i, (color_symbol, color) in enumerate(self.COLOR_MAPPING.items(), start=100):
                curses.init_pair(i, color, -1)
                self.color_cache[color_symbol] = i

    def _select_activity(self):
        """Set the currently selected activity."""
        start, end, self.selected_activity = self.navigation_data[self.current_layer][self.layer_position]

        # Ignore borders, we only care about text
        self.activity_start = Point(start.x + 1, start.y + 1)
        self.activity_end = Point(end.x - 1, end.y - 1)

    def _addstr_with_color_codes(self, window, y: int, x: int, text: str):
        """Replace ANSI color codes with curses colors."""
        if not curses.has_colors():
            window.addstr(y, x, text)
            return

        split_text = text.split("\033")

        # add first part without color
        window.addstr(y, x, split_text[0], curses.color_pair(self.color_cache["[0"]))

        x += len(split_text[0])

        for substring in split_text[1:]:
            color, snippet = substring.split("m", 1)

            if len(snippet) == 0:
                continue

            if color == "[1":
                curses_color = curses.A_BOLD
            else:
                curses_color = curses.color_pair(self.color_cache[color])

            window.addstr(y, x, snippet, curses_color)

            x += len(snippet)

    def _blink_text(self, window, start: Point, end: Point, bold: bool = True):
        """Change text between start and end to blinking."""
        style = curses.A_BLINK

        if bold:
            style |= curses.A_BOLD

        for y in range(start.y, end.y + 1):
            text = window.instr(y, start.x, end.x - start.x + 1)
            window.addstr(y, start.x, text, style)

    def _unblink_text(self, window, start: Point, end: Point, bold: bool = True):
        """Change text between start and end to not-blinking."""

        for y in range(start.y, end.y + 1):
            text = window.instr(y, start.x, end.x - start.x + 1)
            if bold:
                window.addstr(y, start.x, text, curses.A_BOLD)
            else:
                window.addstr(y, start.x, text)

    def _add_multiline_text_with_wrapping(self, window, text: str):
        """Add a multiline text to a window, wrapping text to fit."""
        height, width = window.getmaxyx()

        # don't write to window borders
        width -= 2
        i = 1

        for line in text.splitlines():
            chunks = [line[p : p + width] for p in range(0, len(line), width)]
            if not chunks:
                # Lines containing only \n
                i += 1
                continue

            for chunk in chunks:
                if i >= height:
                    # TODO: Add scrolling using a pad?
                    return
                window.addstr(i, 1, chunk)
                i += 1

    def _refresh(self, screen):
        """Refresh curses screens/pads/windows/panels."""
        self.content_pad.refresh(self.y_pos, self.x_pos, 0, 0, self.rows - 1, self.cols - 1)

        if self.activity_overlay:
            self.activity_overlay.overlay(screen)
            self.activity_overlay.refresh()

        if self.help_overlay:
            self.help_overlay.overlay(screen)
            self.help_overlay.refresh()

        if self.activity_overlay or self.help_overlay:
            curses.panel.update_panels()

    def _change_layer(self, step: int):
        """Change the currently active layer."""
        self.current_layer = max(min(self.current_layer + step, self.max_layer), 0)
        self.layer_position = max(min(self.layer_position, len(self.navigation_data[self.current_layer]) - 1), 0)
        del self.activity_overlay
        self.activity_overlay = None

    def _change_layer_position(self, step: int):
        """Change position inside current layer."""
        self.layer_position = max(min(self.layer_position + step, len(self.navigation_data[self.current_layer]) - 1), 0)
        del self.activity_overlay
        self.activity_overlay = None

    def _update_activity_overlay(self, screen):
        """Show/Hide the activity overlay."""
        if not self.activity_overlay:
            del self.help_overlay
            self.help_overlay = None

            self.activity_overlay = curses.newwin(
                self.ACTIVITY_OVERLAY_HEIGHT,
                self.ACTIVITY_OVERLAY_WIDTH,
                self.rows - self.ACTIVITY_OVERLAY_HEIGHT,
                self.cols - self.ACTIVITY_OVERLAY_WIDTH,
            )
            curses.panel.new_panel(self.activity_overlay)
            self.activity_overlay.border()

            started_date = self.selected_activity.started_at_time.strftime(self.DATE_FORMAT)
            ended_date = self.selected_activity.ended_at_time.strftime(self.DATE_FORMAT)
            usages = "\n".join(u.entity.path for u in self.selected_activity.usages)
            generations = "\n".join(g.entity.path for g in self.selected_activity.generations)
            agents = ", ".join(getattr(a, "full_name", a.name) for a in self.selected_activity.agents)
            full_command = " ".join(self.selected_activity.plan_with_values.to_argv(with_streams=True))

            content = (
                "Id:\n"
                f"{self.selected_activity.id}\n\n"
                "Started:\n"
                f"{started_date}\n\n"
                "Ended:\n"
                f"{ended_date}\n\n"
                "Agents:\n"
                f"{agents}\n\n"
                "Plan Id:\n"
                f"{self.selected_activity.association.plan.id}\n\n"
                "Plan Name:\n"
                f"{self.selected_activity.association.plan.name}\n\n"
                "Inputs:\n"
                f"{usages}\n\n"
                "Outputs:\n"
                f"{generations}\n\n"
                "Full Command:\n"
                f"{full_command}\n\n"
            )

            self._add_multiline_text_with_wrapping(self.activity_overlay, content)
            self.activity_overlay.overlay(screen)
        else:
            del self.activity_overlay
            self.activity_overlay = None

    def _update_help_overlay(self, screen):
        """Show/hide the help overlay."""
        if not self.help_overlay:
            del self.activity_overlay
            self.activity_overlay = None

            self.help_overlay = curses.newwin(
                self.HELP_OVERLAY_HEIGHT, self.HELP_OVERLAY_WIDTH, 0, self.cols - self.HELP_OVERLAY_WIDTH
            )
            curses.panel.new_panel(self.help_overlay)
            self.help_overlay.border()

            content = (
                "Navigate using arrow keys\n"
                "Press <enter> to show activity details\n"
                "Press <h> to show/hide this help\n"
                "Press <q> to exit\n"
            )
            self._add_multiline_text_with_wrapping(self.help_overlay, content)
            self.help_overlay.overlay(screen)
        else:
            del self.help_overlay
            self.help_overlay = None

    def _move_viewscreen(self):
        """Move viewscreen to include selected activity."""
        if self.activity_start.x - 1 < self.x_pos:
            self.x_pos = max(self.activity_start.x - 1, 0)
        elif self.activity_end.x + 1 > self.x_pos + self.cols:
            self.x_pos = min(self.activity_end.x + 1, self.content_max_x) - self.cols - 1

        if self.activity_start.y - 1 < self.y_pos:
            self.y_pos = max(self.activity_start.y - self.vertical_space - 3, 0)
        elif self.activity_end.y + 1 > self.y_pos + self.rows:
            self.y_pos = min(self.activity_end.y + self.vertical_space + 3, self.content_max_y) - self.rows - 1

    def _loop(self, screen):
        """The interaction loop."""
        running = True

        while running:
            input_char = screen.getch()

            # update screen size to deal with resizes
            self.rows, self.cols = screen.getmaxyx()

            # handle keypress
            if input_char == curses.KEY_DOWN or chr(input_char) == "k":
                self._change_layer(1)
            elif input_char == curses.KEY_UP or chr(input_char) == "i":
                self._change_layer(-1)
            elif input_char == curses.KEY_RIGHT or chr(input_char) == "l":
                self._change_layer_position(1)
            elif input_char == curses.KEY_LEFT or chr(input_char) == "j":
                self._change_layer_position(-1)
            elif input_char == curses.KEY_ENTER or input_char == 10 or input_char == 13:
                self._update_activity_overlay(screen)
            elif chr(input_char) == "h":
                self._update_help_overlay(screen)
            elif input_char < 256 and chr(input_char) == "q":
                running = False

            self._unblink_text(self.content_pad, self.activity_start, self.activity_end, bold=True)
            self._select_activity()
            self._blink_text(self.content_pad, self.activity_start, self.activity_end, bold=True)

            self._move_viewscreen()

            self._refresh(screen)

    def _main(self, screen):
        """The main execution method for wrapped curses execution."""
        self._init_curses(screen)
        self._loop(screen)

    def run(self):
        """Run interactive curses mode."""
        curses.wrapper(self._main)
