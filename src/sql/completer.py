from __future__ import print_function

from pgcli.pgcompleter import PGCompleter
from prompt_toolkit.document import Document


completer = PGCompleter()


def sql_completer(self, event):
    completions = completer.get_completions(
        Document(
            text=event.line,
            cursor_position=len(event.text_until_cursor)
        ),
        None
    )
    prefix = ''
    if '.' in event.symbol:
        prefix = event.symbol.rsplit('.', 1)[0] + '.'
    return [(prefix + c.display) for c in completions]
