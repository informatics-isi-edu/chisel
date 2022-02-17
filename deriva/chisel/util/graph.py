"""Methods for graphing a catalog model."""

from graphviz import Digraph


def graph(obj, engine='fdp'):
    """Generates and returns a graphviz Digraph.

    :param obj: a catalog model object
    :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
    :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
    """
    if hasattr(obj, 'schemas'):
        return graph_model(obj, engine=engine)
    elif hasattr(obj, 'tables'):
        return graph_schema(obj, engine=engine)
    elif hasattr(obj, 'columns'):
        return graph_table(obj, engine=engine)

    return TypeError('Objects of type {typ} are not supported'.format(typ=type(obj).__name__))


def graph_model(model, engine='fdp'):
    """Generates and returns a graphviz Digraph.

    :param model: a catalog model
    :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
    :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
    """
    dot = Digraph(name='Catalog Model', engine=engine, node_attr={'shape': 'box'})
    dot.attr('graph', overlap='false', splines='true')

    # add nodes
    for schema in model.schemas.values():
        with dot.subgraph(name=schema.name, node_attr={'shape': 'box'}) as subgraph:
            for table in schema.tables.values():
                label = "%s.%s" % (schema.name, table.name)
                subgraph.node(label, label)

    # add edges
    for schema in model.schemas.values():
        for table in schema.tables.values():
            tail_name = "%s.%s" % (schema.name, table.name)
            for fkey in table.foreign_keys:
                refcol = fkey.referenced_columns[0]
                head_name = "%s.%s" % (refcol.table.schema.name, refcol.table.name)
                dot.edge(tail_name, head_name)

    return dot


def graph_schema(schema, engine='fdp'):
    """Generates and returns a graphviz Digraph.

    :param schema: a catalog schema object
    :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
    :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
    """
    dot = Digraph(name=schema.name, engine=engine, node_attr={'shape': 'box'})
    dot.attr('graph', overlap='false', splines='true')

    # add nodes
    for table in schema.tables.values():
        label = "%s.%s" % (schema.name, table.name)
        dot.node(label, label)

    # track referenced nodes
    seen = set()

    # add edges
    for table in schema.tables.values():
        # add outbound edges
        tail_name = "%s.%s" % (schema.name, table.name)
        for fkey in table.foreign_keys:
            refcol = fkey.referenced_columns[0]
            head_name = "%s.%s" % (refcol.table.schema.name, refcol.table.name)
            # add head node, if not seen
            if head_name not in seen:
                seen.add(head_name)
                dot.node(head_name, head_name)
            # add edge, if not seen before
            edge = (tail_name, head_name)
            if edge not in seen:
                seen.add(edge)
                dot.edge(tail_name, head_name)

        # add inbound edges
        head_name = tail_name
        for reference in table.referenced_by:
            fkeycol = reference.foreign_key_columns[0]
            tail_name = "%s.%s" % (fkeycol.table.schema.name, fkeycol.table.name)
            # add tail node, if not seen
            if tail_name not in seen:
                seen.add(tail_name)
                dot.node(tail_name, tail_name)
            # add head node, if not seen
            edge = (tail_name, head_name)
            if edge not in seen:
                seen.add(edge)
                dot.edge(tail_name, head_name)

    return dot


def graph_table(table, engine='fdp'):
    """Generates and returns a graphviz Digraph.

    :param table: a catalog table object
    :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
    :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
    """
    dot = Digraph(name=table.name, engine=engine, node_attr={'shape': 'box'})
    dot.attr('graph', overlap='false', splines='true')

    # add node
    label = "%s.%s" % (table.schema.name, table.name)
    dot.node(label, label)

    # track referenced nodes
    seen = set()

    # add edges
    # add outbound edges
    tail_name = "%s.%s" % (table.schema.name, table.name)
    for fkey in table.foreign_keys:
        refcol = fkey.referenced_columns[0]
        head_name = "%s.%s" % (refcol.table.schema.name, refcol.table.name)
        if head_name not in seen:
            dot.node(head_name, head_name)
            seen.add(head_name)
        dot.edge(tail_name, head_name)

    # add inbound edges
    head_name = tail_name
    for reference in table.referenced_by:
        fkeycol = reference.foreign_key_columns[0]
        tail_name = "%s.%s" % (fkeycol.table.schema.name, fkeycol.table.name)
        if tail_name not in seen:
            dot.node(tail_name, tail_name)
            seen.add(tail_name)
        dot.edge(tail_name, head_name)

    return dot
