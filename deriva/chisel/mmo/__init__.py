"""Model management operators.
"""
from collections import namedtuple
import sys
from deriva.core import tag as tags

Match = namedtuple('Match', 'anchor tag context container mapping')

__search_box__ = 'search-box'


def prune(model, symbol):
    """Prunes mappings from a model where symbol found in mapping.

    See the 'find' function for notes on what are 'symbols' and how symbols are found in the model.

    **Note on pruning source definitions**
    When this methods prunes a `source-definitions` entry from the model, it will also prune
    `visible-{columns|foreign-keys}` that reference its `sourcekey` directly or as a "path prefix" in a pseudo-column
    definition. It will prune any references found in `wait_for` display attributes. Also, it will recurse over the
    source definitions repeating the above pruning for each sourcekey dependent on the originally affected sourcekey.
    """
    # step 1: find all matches
    for anchor, tag, context, container, mapping in find(model, symbol):
        # step 2: remove mapping from model, for each mapping found
        if tag in [tags.visible_columns, tags.visible_foreign_keys]:
            container.remove(mapping)
        elif tag == tags.source_definitions:
            print(f'Remove from {tag}, sourcekey {mapping} and dependencies')
            # step 2.a. find all instances of sourcekey and sourcekeys that depend on sourcekey in anchor's annotations
            for match in _find_sourcekey(anchor, mapping):
                # step 2.b. remove all found depenencies on sourcekey
                print(match.tag, match.mapping)
                if match.tag in [tags.visible_columns, tags.visible_foreign_keys]:
                    match.container.remove(match.mapping)
                elif match.tag == tags.citation:
                    del match.anchor.annotations[match.tag]
                elif match.tag == tags.source_definitions:
                    del match.container[match.mapping]
                else:
                    print(f'WARNING: unexpected tag {match.tag}', file=sys.stderr)
            # step 2.c. remove sourcekey
            del container[mapping]
        else:
            print(f'Unhandled tag "{tag}"', file=sys.stderr)


def find(model, symbol):
    """Finds mappings within a model where symbol found in mapping.

    Searches the following annotation tags:
    - source-definitions
    - visible-columns
    - visible-foreign-keys

    Presently, there are two forms of symbols:
    - constrain [schema_name, constraint_name] may be a key or fkey
    - column [schema_name, table_name, column_name]

    If/when table must be supported, the ambiguity could be addressed as:
    - table [schema_name, table_name, None] where the final None in the column category implies that we are removing
      not a single column but the whole table (and hence all of its columns)

    returns: list containing Match(anchor, tag, context, container, mapping) tuples
    - anchor: the model object that anchors the mapping
    - tag: the annotation tag where the mapping was found
    - context: the annotation context where the mapping was found  # todo: tbd whether this is really needed by the caller
    - container: the parent container of the mapping  # simplifies pruning of mapping
    - mapping: the mapping in which the symbol was found  # simplifies renaming of symbol in mapping
    """
    matches = []

    # At present, mappings only reside in model elements: table and column.
    for schema in model.schemas.values():
        for table in schema.tables.values():
            for tag in table.annotations:

                # case: visible-columns or visible-foreign-keys
                if tag == tags.visible_columns or tag == tags.visible_foreign_keys:
                    for context in table.annotations[tag]:
                        if context == 'filter':
                            vizsrcs = table.annotations[tag][context].get('and', [])
                        else:
                            vizsrcs = table.annotations[tag][context]

                        for vizsrc in vizsrcs:  # vizsrc is a vizcol or vizfkey entry
                            # case: constraint form of vizsrc
                            if isinstance(vizsrc, list) \
                                    and vizsrc == symbol:
                                matches.append(Match(table, tag, context, vizsrcs, vizsrc))
                            # case: pseudo-column form of vizsrc
                            elif isinstance(vizsrc, dict) and 'source' in vizsrc \
                                    and _is_symbol_in_source(table, vizsrc['source'], symbol):
                                matches.append(Match(table, tag, context, vizsrcs, vizsrc))
                            # case: column form of vizsrc
                            elif isinstance(vizsrc, str) \
                                    and [table.schema.name, table.name, vizsrc] == symbol:
                                matches.append(Match(table, tag, context, vizsrcs, vizsrc))

                # case: source-definitions
                elif tag == tags.source_definitions:
                    # search 'columns'
                    cols = table.annotations[tag].get('columns')
                    if isinstance(cols, list) \
                            and len(symbol) == 3 \
                            and (table.schema.name, table.name) == symbol[0:2] \
                            and symbol[-1] in cols:
                        matches.append(Match(table, tag, None, cols, symbol[-1]))

                    # search 'fkeys'
                    fkeys = table.annotations[tag].get('fkeys')
                    if isinstance(fkeys, list):
                        for fkey in fkeys:
                            if fkey == symbol:
                                matches.append(Match(table, tag, None, fkeys, fkey))

                    # search 'sources'
                    sources = table.annotations[tag].get('sources')
                    for sourcekey in sources:
                        if _is_symbol_in_source(table, sources[sourcekey].get('source', []), symbol):
                            matches.append(Match(table, tag, None, sources, sourcekey))

    return matches


def _is_symbol_in_source(table, source, symbol):
    """Finds symbol in a source mapping.
    """

    # case: source is a column name
    if isinstance(source, str):
        return [table.schema.name, table.name, source] == symbol

    # case: source is a path, symbol is a constraint
    if isinstance(source, list) and isinstance(symbol, list):

        # case: symbol is a constraint
        if len(symbol) == 2:
            for pathelem in source:
                if isinstance(pathelem, dict):
                    constraint_name = pathelem.get('inbound') or pathelem.get('outbound')
                    if constraint_name == symbol:
                        return True

        # case: symbol is a column name
        #  -- start with column in path and test if matches last value of source path
        #  -- then determine if the table reference matches (last fkey in/out points to correct table)
        #  -- isinstance(source[-1], str) and source[-1] == symbol[-1]
        #  -- isinstance(source[-2], dict) -- ie, a constraint
        #  -- lookup fkey = model.fkeys(*source[-2].get('inbound' or 'outbound')
        #  -- if 'inbound' and fkey.table == symbol's table
        #  -- if 'outbound' and fkey.pk_table == symbol's table
        if len(symbol) == 3 \
            and len(source) >= 2 \
            and source[-1] == symbol[-1] \
            and isinstance(source[-2], dict):

            # case: inbound fkey
            if 'inbound' in source[-2]:
                fkey = table.schema.model.fkey(source[-2]['inbound'])
                return [fkey.table.schema.name, fkey.table.name] == symbol[0:2]

            # case: outbound fkey
            elif 'outbound' in source[-2]:
                fkey = table.schema.model.fkey(source[-2]['outbound'])
                return [fkey.pk_table.schema.name, fkey.pk_table.name] == symbol[0:2]

    return False


def _find_sourcekey(table, sourcekey):
    """Find usages of `sourcekey` in `table`'s annotations.
    """
    matches = []

    # case: source-definitions
    #                           sources, <sourcekey>, display, wait_for (markdown_pattern)
    #                                                 source, [0], sourcekey
    #
    # case: citation
    #                 wait_for (*_pattern)
    #
    # case: visible-columns or visible-foreign-keys
    #                        <context>, <pseudocolumn>, sourcekey
    #                                                   source, [0], sourcekey
    #                                                   display, wait_for (markdown_pattern)

    # step 1: find all references to sourcekey in source-definitions (recursively)
    sources = table.annotations[tags.source_definitions].get('sources', {})
    sourcekeys = _find_dependent_sourcekeys(sourcekey, sources)
    matches.extend([
        Match(table, tags.source_definitions, None, sources, sourcekey)
        for sourcekey in sourcekeys
    ])

    # step 2: find all other references to the set of {sourcekeys} | {sourcekey} found in first step
    sourcekeys.add(sourcekey)

    #      2.a.: find sourcekey in citations
    citation = table.annotations.get(tags.citation)
    if citation:
        for sourcekey in sourcekeys:
            if sourcekey in citation.get('wait_for', []):
                matches.append(Match(table, tags.citation, None, None, None))
                break

    #      2.b.: find sourcekey in visible-columns or visible-foreign-keys
    for tag in (tags.visible_columns, tags.visible_foreign_keys):
        for context in table.annotations.get(tag, {}):
            vizcols = table.annotations[tag][context]
            for vizcol in vizcols:
                for sourcekey in sourcekeys:
                    if _is_dependent_on_sourcekey(sourcekey, vizcol):
                        matches.append(Match(table, tag, context, vizcols, vizcol))

    return matches


def _find_dependent_sourcekeys(sourcekey, sources, deps=None):
    """Find 'sourcekey' dependencies in 'sources' source definitions.
    """
    # initialize deps to empty set
    deps = deps or set()

    # remove self from sources
    if sourcekey in sources:
        sources = sources.copy()
        sources.pop(sourcekey)

    # test if each 'candidate' source is dependent on this 'sourcekey'
    for candidate in sources:
        # case: source 'search-box', need to check sources in its `or` block, but do not recurse
        if candidate == __search_box__:
            for source_def in sources[__search_box__].get('or', []):
                if _is_dependent_on_sourcekey(sourcekey, source_def):
                    # add 'search-box' to deps, continue
                    deps.add(__search_box__)
                    break
        # case: all other sources
        if _is_dependent_on_sourcekey(sourcekey, sources[candidate]):
            # add to deps
            deps.add(candidate)
            # union with deps of the candidate
            deps.union(_find_dependent_sourcekeys(candidate, sources, deps=deps))

    return deps


def _is_dependent_on_sourcekey(sourcekey, source_def):
    """Tests if 'source_def' is dependent on 'sourcekey'.
    """
    # case: source_def is not a pseudo-column
    if not isinstance(source_def, dict):
        return False
    # case: sourcekey referenced directly
    if sourcekey == source_def.get('sourcekey'):
        return True
    # case: sourcekey in path prefix
    if not isinstance(source_def.get('source'), str) and sourcekey == source_def.get('source', [{}])[0].get('sourcekey'):
        return True
    # case: sourcekey in wait_for
    if sourcekey in source_def.get('display', {}).get('wait_for', []):
        return True
    # not dependent
    return False
