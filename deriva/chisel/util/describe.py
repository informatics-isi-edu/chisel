"""Methods for describing a catalog model."""

from collections import defaultdict


def describe(obj):
    """Returns a text (markdown) description.

    :param obj: a catalog model object
    :return: a Description object that can be dumped to the console for text or markdown display
    """
    if hasattr(obj, 'schemas'):
        return describe_catalog(obj)
    elif hasattr(obj, 'tables'):
        return describe_schema(obj)
    elif hasattr(obj, 'columns'):
        return describe_table(obj)

    return TypeError('Objects of type {typ} are not supported'.format(typ=type(obj).__name__))


def describe_catalog(model):
    """Returns a text (markdown) description.

    :param model: a catalog model
    :return: a Description object that can be dumped to the console for text or markdown display
    """

    def _make_markdown_repr(quote=lambda s: s):
        data = [
                   ["Name", "Comment"]
               ] + [
                   [s.name, s.comment] for s in model.schemas.values()
               ]
        desc = "### List of schemas\n" + \
               markdown_table(data, quote)
        return desc

    class Description:
        def _repr_markdown_(self):
            return _make_markdown_repr(quote=markdown_quote)

        def __repr__(self):
            return _make_markdown_repr()

    return Description()


def describe_schema(schema):
    """Returns a text (markdown) description.

    :param schema: a catalog schema object
    :return: a Description object that can be dumped to the console for text or markdown display
    """

    def _make_markdown_repr(quote=lambda s: s):
        data = [
            ["Schema", "Name", "Kind", "Comment"]
        ] + [
            [schema.name, t.name, t.kind, t.comment] for t in schema.tables.values()
        ]
        desc = "### List of Tables\n" + \
               markdown_table(data, quote)
        return desc

    class Description:
        def _repr_markdown_(self):
            return _make_markdown_repr(quote=markdown_quote)

        def __repr__(self):
            return _make_markdown_repr()

    return Description()


def describe_table(table):
    """Returns a text (markdown) description.

    :param table: a catalog table object
    :return: a Description object that can be dumped to the console for text or markdown display
    """

    def _make_markdown_repr(quote=lambda s: s):
        data = [
            ["Name", "Type", "Nullable", "Default", "Comment"]
        ] + [
            [col.name, col.type.typename, str(col.nullok), col.default, col.comment] for col in table.columns
        ]
        desc = "### Table \"" + quote(str(table.schema.name)) + ":" + quote(str(table.name)) + "\"\n" + \
               "#### Columns\n" + \
               markdown_table(data, quote) + "\n"

        if table.keys:
            key_table = [
                ["Constraint Name", "Unique Columns"]
            ] + [
                [key.constraint_name, ", ".join(["%s" % c.name for c in key.unique_columns])] for key in table.keys
            ]
            desc += "#### Keys\n" + \
                    markdown_table(key_table, quote) + "\n"

        if table.foreign_keys:
            fkey_table = [
                ["Constraint Name", "Foreign Key Columns", "Table", "Referenced Columns"]
            ] + [
                [
                    fkey.constraint_name,
                    ", ".join(["%s" % c.name for c in fkey.foreign_key_columns]),
                    "%s:%s" % (fkey.pk_table.schema.name, fkey.pk_table.name),
                    ", ".join(["%s" % c.name for c in fkey.referenced_columns])
                ]
                for fkey in table.foreign_keys
            ]
            desc += "#### Foreign Keys\n" + \
                    markdown_table(fkey_table, quote) + "\n"

        if table.referenced_by:
            refby_table = [
                ["Constraint Name", "Table", "Foreign Key Columns", "Referenced Columns"]
            ] + [
                [
                    fkey.constraint_name,
                    "%s:%s" % (fkey.table.schema.name, fkey.table.name),
                    ", ".join(["%s" % c.name for c in fkey.foreign_key_columns]),
                    ", ".join(["%s" % c.name for c in fkey.referenced_columns])
                ]
                for fkey in table.referenced_by
            ]
            desc += "#### Referenced By\n" + \
                    markdown_table(refby_table, quote) + "\n"

        return desc

    class Description:
        def _repr_markdown_(self):
            return _make_markdown_repr(quote=markdown_quote)

        def __repr__(self):
            return _make_markdown_repr()

    return Description()


def markdown_quote(s, special="\\`*_{}[]()#+-.!"):
    """Simple markdown quoting that returns a new encoded string for the original input string."""
    if not s:
        return s

    t = ""
    for c in s:
        if c in special:
            t += '\\'
        t += c
    return t


def markdown_table(data=[[""]], quote=lambda s: s):
    """Generates markdown table from input data."""

    # convert data into text
    text = [list(map(lambda x: str(x), row)) for row in data]

    # determine the padding for each column
    padding = defaultdict(int)
    for row in text:
        for i, value in enumerate(row):
            padding[i] = max(padding[i], len(value))

    # generate the markdown table
    table = '| ' + ' | '.join([quote(val).ljust(padding[i]) for i, val in enumerate(text[0])]) + ' |\n' + \
            '|-' + '-|-'.join([''.ljust(padding[i], '-') for i in range(len(text[0]))]) + '-|\n' + \
            ''.join([
                '| ' + ' | '.join([(quote(val)).ljust(padding[i]) for i, val in enumerate(row)]) + ' |\n'
                for row in text[1:]
            ])
    return table