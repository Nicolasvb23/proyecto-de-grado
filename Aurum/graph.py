import pickle
import os
from d3l.input_output.dataloaders import CSVDataLoader
import networkx as nx
from d3l.querying.query_engine import QueryEngine
import plotly.graph_objs as go
import plotly.offline as pyo
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go2

from rdflib import Graph, Namespace, URIRef, RDF
from rdflib.namespace import DCAT, DCTERMS




def average_score(Neighbour_score, threshold=0.5):
    """
    This function calculates the average syntactic similarity scores of attribute header
    and attribute values.
    if all of attribute header/values similarity scores are 0, we will only consider the other
    score, otherwise we will consider the average syntactic similarity.
    :param Neighbour_score: the score tuple of results
    :param threshold: similarity threshold
    returns: the average syntactic similarity
    """
    similarities_value = [similarities[1] for name, similarities in Neighbour_score]
    if all(x == 0 for x in similarities_value):
        return [(name, similarities[0]) for name, similarities in Neighbour_score]
    else:
        return [(name, sum(similarities) / len(similarities)) for name, similarities
                in Neighbour_score if sum(similarities) / len(similarities) > threshold]


def node_in_graph(dataloader: CSVDataLoader, table_dict=None):
    """
    This function is to load all columns in the datasets as node
    in the graph and return the graph
    each node will store basic information like node name {table_name}.{column_name}, column_type, if it is a subject
    column
    :param dataloader: class that is used to load all data
    :param table_dict: the dictionary that contains the column type dict and the Named-entity columns scores of each
    table calculated by tableMiner+
    """
    graph = nx.Graph()
    tables = table_dict.keys()
    for table_name in tables:
        short_name = table_name[:-4]
        table = dataloader.read_table(table_name=short_name)
        column_t = table.columns
        annotation, NE_scores = table_dict[table_name]
        subCol_index = max(NE_scores, key=lambda k: NE_scores[k]) if len(NE_scores) > 0 else None
        for index, col in enumerate(column_t):
            graph.add_node(f"{short_name}.{col}", table_name=short_name, column_type=annotation[index])
            if index == subCol_index:
                graph.nodes[f"{short_name}.{col}"]['SubjectColumn'] = True
    return graph


def buildGraph(dataloader, data_path, indexes, target_path, table_dict=None, similarity_threshold=0.5):
    """
    This function is to build the AURUM graph based on the given indexes.
    :param dataloader: class that is used to load all data
    :param data_path: the data path of all datasets
    :param indexes: the LSH indexes
    :param target_path: where the graph will be saved
    :param table_dict: the dictionary that contains the column type dict and the Named-entity columns scores of each
    table calculated by tableMiner+
    """
    if os.path.exists(os.path.join(target_path, "AurumOnto.pkl")):
        with open(os.path.join(target_path, "AurumOnto.pkl"), "rb") as save_file:
            graph = pickle.load(save_file)
    else:
        graph = node_in_graph(dataloader, table_dict)
        print("start ...")
        T = [i for i in os.listdir(data_path) if i.endswith(".csv")]
        columns = []
        for t in T:
            short_name = t[:-4]
            table = dataloader.read_table(table_name=t[:-4])
            column_t = table.columns
            for col in column_t:
                columns.append((f"{short_name}.{col}", table[col]))
        for col_tuple in columns:
            col_name, column = col_tuple
            qe = QueryEngine(*indexes)
            Neighbours = qe.column_query(column, aggregator=None)
            all_neighbours = average_score(Neighbours, threshold=similarity_threshold)
            isSubCol = graph.nodes[col_name].get('SubjectColumn', False)
            type = "Syntactic_Similar" if isSubCol is False else "PK_FK"
            for neighbour_node, score in all_neighbours:
                if graph.has_edge(col_name, neighbour_node) is False:
                    graph.add_edge(col_name, neighbour_node, weight=score, type=type)
        with open(os.path.join(target_path, "AurumOnto.pkl"), "wb") as save_file:
            pickle.dump(graph, save_file)

    return graph


def draw_interactive_network(G):
    # Get the layout of graph G
    pos = nx.spring_layout(G)

    # Prepare node drawing information
    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))

    for node in G.nodes():
        x, y = pos[node]
        node_trace['x'] += (x,)
        node_trace['y'] += (y,)

        # Add a text label to each node showing all attributes
        node_info = f'{node}<br>' + '<br>'.join([f'{key}: {value}' for key, value in G.nodes[node].items()])
        node_trace['text'] += (node_info,)

    # Prepare edge drawing information
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=0.5, color='#888'),
        hoverinfo='text',
        mode='lines',
        text=[])

    # Add edge start and end locations
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_trace['x'] += (x0, x1, None)
        edge_trace['y'] += (y0, y1, None)

        # Add a text label to each edge showing all properties
        edge_info = f'{edge}<br>' + '<br>'.join([f'{key}: {value}' for key, value in G.edges[edge].items()])
        edge_trace['text'] += (edge_info,)

    # Create chart
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Subgraph',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    # show the chart
    pyo.iplot(fig)


def save_graph(graph, path):
    # Carpeta donde guardarás el archivo
    folder_name = path
    os.makedirs(folder_name, exist_ok=True)  # Crear la carpeta si no existe

    # Ruta completa del archivo
    file_path = os.path.join(folder_name, "grafo.pkl")

    # Guardar el grafo en el archivo
    with open(file_path, "wb") as f:
        pickle.dump(graph, f)

    print(f"Grafo guardado en '{file_path}'.")

def load_graph(path):
    # Cargar el grafo desde el archivo
    with open(path, "rb") as f:
        loaded_graph = pickle.load(f)


    return loaded_graph


def draw_interactive_network_with_filters(G):
    pos = nx.spring_layout(G)

    # Crear listas iniciales para nodos y aristas
    node_x = []
    node_y = []
    node_text = []
    node_color = []
    edge_x = []
    edge_y = []
    edge_text = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        connections = len(list(G.neighbors(node)))
        node_color.append(connections)
        node_info = f'{node}<br>' + '<br>'.join([f'{key}: {value}' for key, value in G.nodes[node].items()])
        node_text.append(node_info)

    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        edge_info = f'{edge}<br>' + '<br>'.join([f'{key}: {value}' for key, value in G.edges[edge].items()])
        edge_text.append(edge_info)

    # Crear mapa de nodos a índices
    node_to_index = {node: i for i, node in enumerate(G.nodes())}

    # Crear trazo para nodos
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        text=node_text,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            color=node_color,
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))

    # Crear trazo para aristas
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='text',
        mode='lines',
        text=edge_text)

    # Layout base
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Red Interactiva con Filtros',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    # Filtros dinámicos
    buttons = []

    # Filtro: Mostrar todos
    buttons.append(dict(
        label="Mostrar Todo",
        method="update",
        args=[{"x": [edge_x, node_x],
               "y": [edge_y, node_y],
               "visible": [True, True]},  # Nodo y arista visibles
              {"title": "Red Completa"}]
    ))

    # Filtro: Nodos con ≥ min_connections conexiones
    min_connections = 3  # Cambia según lo que quieras filtrar
    filtered_node_x = []
    filtered_node_y = []
    filtered_node_text = []
    filtered_node_color = []
    filtered_edge_x = []
    filtered_edge_y = []

    for node in G.nodes():
        if len(list(G.neighbors(node))) >= min_connections:
            x, y = pos[node]
            filtered_node_x.append(x)
            filtered_node_y.append(y)
            filtered_node_color.append(len(list(G.neighbors(node))))
            filtered_node_text.append(f'{node}<br>' + '<br>'.join([f'{key}: {value}' for key, value in G.nodes[node].items()]))

    for edge in G.edges():
        if all(len(list(G.neighbors(n))) >= min_connections for n in edge):
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            filtered_edge_x.extend([x0, x1, None])
            filtered_edge_y.extend([y0, y1, None])

    buttons.append(dict(
        label=f"Nodos con ≥ {min_connections} conexiones",
        method="update",
        args=[{"x": [filtered_edge_x, filtered_node_x],
               "y": [filtered_edge_y, filtered_node_y],
               "visible": [True, True]},  # Nodo y arista visibles
              {"title": f"Nodos con ≥ {min_connections} conexiones"}]
    ))

    # Agregar los botones al layout
    fig.update_layout(
        updatemenus=[dict(
            type="buttons",
            direction="left",
            buttons=buttons,
            showactive=True,
            x=0.1,
            y=1.2
        )]
    )

    # Mostrar el gráfico

    pyo.iplot(fig)


def draw_D3L_graph(data_list, G=None, dibujar=False):
    # Crear un nuevo grafo si no se proporciona uno
    if G is None:
        G = nx.Graph()

    # Nodo principal (primero en data_list)
    main_table = data_list[0][0]  # 'T2DV2_1'
    if not G.has_node(main_table):
        G.add_node(main_table)

    # Conectar el nodo principal con las demás tablas
    for item in data_list[1:]:
        table = item[0]  # Extraer el nombre de la tabla
        if not G.has_node(table):
            G.add_node(table)
        if not G.has_edge(main_table, table):
            G.add_edge(main_table, table)

    # Calcular posiciones de los nodos (solo para visualización)
    pos = nx.spring_layout(G)

    # Listas para nodos y aristas
    node_x = []
    node_y = []
    node_text = []
    edge_x = []
    edge_y = []

    # Procesar nodos
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_info = f'{node}<br>Conexiones: {len(list(G.neighbors(node)))}'
        node_text.append(node_info)

    # Procesar aristas
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Crear trazo para aristas
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )

    # Crear trazo para nodos
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale='Inferno',
            size=10,
            color=[len(list(G.neighbors(node))) for node in G.nodes()],
            colorbar=dict(
                thickness=15,
                title='Conexiones',
                xanchor='left',
                titleside='right'
            )
        )
    )

    # Layout base
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Tablas relacionadas en base a D3L',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    # Mostrar el grafo
    if dibujar:
        fig.show()

    # Devolver el grafo para futuras iteraciones
    return G


# Se debe ejecutar luego de la funcion draw_D3L_graph
def draw_table_miner_graph(G, tableminer_edges=[], dibujar=False):
    
    # Asegurarse de que las aristas de TableMiner estén en el grafo
    for edge in tableminer_edges:
        if not G.has_edge(*edge):
            G.add_edge(*edge)

    # Calcular posiciones de los nodos
    pos = nx.spring_layout(G)

    # Listas para nodos y aristas
    node_x = []
    node_y = []
    node_text = []
    d3l_edge_x = []
    d3l_edge_y = []
    tableminer_edge_x = []
    tableminer_edge_y = []

    # Procesar nodos
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_info = f'{node}<br>Conexiones: {len(list(G.neighbors(node)))}'
        node_text.append(node_info)

    # Procesar aristas
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]

        # Aristas de TableMiner
        if edge in tableminer_edges or (edge[1], edge[0]) in tableminer_edges:
            tableminer_edge_x.extend([x0, x1, None])
            tableminer_edge_y.extend([y0, y1, None])
        
        # Aristas de D3L (siempre procesar, incluso si ya está en TableMiner)
        d3l_edge_x.extend([x0, x1, None])
        d3l_edge_y.extend([y0, y1, None])

    # Crear trazo para aristas generadas por D3L
    d3l_edge_trace = go.Scatter(
        x=d3l_edge_x,
        y=d3l_edge_y,
        line=dict(width=0.8, color='blue'),  # Color azul para D3L
        hoverinfo='none',
        mode='lines',
        name='Aristas D3L'
    )

    # Crear trazo para aristas generadas por TableMiner
    tableminer_edge_trace = go.Scatter(
        x=tableminer_edge_x,
        y=tableminer_edge_y,
        line=dict(width=0.8, color='red'),  # Color rojo para TableMiner
        hoverinfo='none',
        mode='lines',
        name='Aristas TableMiner'
    )

    # Crear trazo para nodos
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale='Inferno',
            size=10,
            color=[len(list(G.neighbors(node))) for node in G.nodes()],
            colorbar=dict(
                thickness=15,
                title='Conexiones',
                xanchor='left',
                titleside='right'
            )
        )
    )

    # Layout base
    fig = go.Figure(data=[d3l_edge_trace, tableminer_edge_trace, node_trace],
                    layout=go.Layout(
                        title='Tablas relacionadas (D3L y TableMiner)',
                        titlefont_size=16,
                        showlegend=True,
                        legend=dict(
                            x=0,
                            y=-0.1,
                            orientation='h',
                            traceorder='normal'
                        ),
                        hovermode='closest',
                        margin=dict(b=40, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    # Mostrar el grafo
    if dibujar:
        fig.show()

def generate_graph_edges(all_annotations):
    edges = []
    # Crear un diccionario para agrupar tablas por tipo semántico
    semantic_to_tables = {}

    for table, semantics in all_annotations:
        for semantic in semantics:  # Iterar sobre los conceptos dentro del conjunto
            if semantic not in semantic_to_tables:
                semantic_to_tables[semantic] = []
            semantic_to_tables[semantic].append(table)

    # Generar las aristas basadas en las tablas que comparten un tipo semántico
    for semantic, tables in semantic_to_tables.items():
        # Conectar todas las tablas que comparten este tipo semántico
        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                edge = (tables[i], tables[j])
                if edge not in edges:
                    edges.append(edge)

    return edges


def graph_to_dcat_rdf(G):
    # Crear un grafo RDF
    g = Graph()

    # Definir un namespace propio
    EX = Namespace("http://example.org/")

    # Añadir prefijos para facilitar lectura
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("ex", EX)

    # Crear un recurso DCAT:Dataset por cada nodo
    for node in G.nodes():
        g.add((EX[node], RDF.type, DCAT.Dataset))
        # Aquí podrías añadir más metadatos a cada dataset, por ejemplo:
        # g.add((EX[node], DCTERMS.title, Literal(node)))
        # g.add((EX[node], DCTERMS.description, Literal("Descripción del dataset " + node)))

    # Añadir relaciones entre datasets (aristas)
    for n1, n2 in G.edges():
        g.add((EX[n1], DCTERMS.relation, EX[n2]))
        # Si quieres que la relación sea bidireccional a nivel RDF:
        g.add((EX[n2], DCTERMS.relation, EX[n1]))

    # Serializar a Turtle
    return g.serialize(format='turtle')
