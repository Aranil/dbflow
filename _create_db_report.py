'''
this reads the db schema and plots a connection graph of the tables
'''


import config as cfg
from db_utility import connect2db
import pandas as pd
import utils
import logging
import networkx as nx
import matplotlib.pyplot as plt




def get_table_relationships(db_path, conn, excluded_prefixes):
    relationships = []
    primary_keys = {}

    # Get the list of tables
    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(tables_query, conn)['name'].tolist()

    # Filter tables to exclude those with specific prefixes
    filtered_tables = [table for table in tables if not any(table.startswith(prefix) for prefix in excluded_prefixes)]

    for table in filtered_tables:
        # Query the table to check if it's empty
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 1;", conn)
        if df.empty:
            continue  # Skip this table if it is empty

        # Get primary key information
        pk_query = f"PRAGMA table_info({table});"
        pk_info = pd.read_sql_query(pk_query, conn)
        pks = pk_info[pk_info['pk'] > 0]['name'].tolist()
        primary_keys[table] = pks

        # Get foreign key information for each table
        fk_query = f"PRAGMA foreign_key_list({table});"
        fk_info = pd.read_sql_query(fk_query, conn)

        for _, row in fk_info.iterrows():
            relationships.append({
                'Source Table': table,
                'Source Column': row['from'],
                'Target Table': row['table'],
                'Target Column': row['to']
            })

    return pd.DataFrame(relationships), primary_keys


def plot_relationship_graph(relationships_df, primary_keys, group_colors, label_font_size=14,
                            pk_font_size=10, figsize=(14, 14), layout_type='shell', save_to=None):
    G = nx.DiGraph()

    # Group nodes by their assigned colors
    grouped_nodes = {}
    for table, color in group_colors.items():
        if color not in grouped_nodes:
            grouped_nodes[color] = []
        grouped_nodes[color].append(table)

    # Add nodes and apply colors based on groups
    node_colors = []
    for color, tables in grouped_nodes.items():
        for table in tables:
            G.add_node(table)
            node_colors.append(color)

    # Add edges based on relationships
    for _, row in relationships_df.iterrows():
        G.add_edge(row['Source Table'], row['Target Table'],
                   label=f"{row['Source Column']} -> {row['Target Column']}")

    # Use shell_layout to group nodes by their assigned color groups
    if layout_type == 'shell':
        shells = [grouped_nodes[color] for color in grouped_nodes]
        pos = nx.shell_layout(G, nlist=shells)
    elif layout_type == 'circular':
        pos = nx.circular_layout(G)
    elif layout_type == 'spring':
        pos = nx.spring_layout(G, k=2, iterations=100)
    elif layout_type == 'kamada_kawai':
        pos = nx.kamada_kawai_layout(G)
    else:
        pos = nx.spring_layout(G)

    plt.figure(figsize=figsize)

    # Draw nodes
    nx.draw(G, pos, with_labels=False, arrows=True, node_size=3000, node_color=node_colors)

    # Draw edges
    edge_labels = nx.get_edge_attributes(G, 'label')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=pk_font_size)

    # Manually add node labels with different font sizes and alignment
    for node, (x, y) in pos.items():
        # Draw table name larger, positioned above the node
        plt.text(x, y + 0.04, node, fontsize=label_font_size, ha='center', fontweight='bold',
                 verticalalignment='bottom')

        # Draw primary keys smaller, positioned directly below the table name
        if node in primary_keys:
            pk_label = "\n".join([f"PK: {col}" for col in primary_keys[node]])
            plt.text(x, y + 0.03, pk_label, fontsize=pk_font_size, ha='center', verticalalignment='top')

    if save_to:
        plt.savefig(save_to)
        plt.show()



#appcode
#appimagecatalogue
#appobservations

dwd_data = [
            'dwdcatalogue',
            'dwdparametercode',
            'phenoanomaly',
            'phenobserv',
            'phenobservations',
            'phenodata',
            'phenometadata',
            'phenoobjectcode',
            'phenophasecode',
            'phenostations',

            'weatherdata',
            'weathermetadata',
            'weatherstations',
            ]

#aoiaspect
study_area = [
            'areaofinterest',
            'croplegend',
            'fboundaries',
            'aoirowdirection',
            'aoilegend',
            ]

managment_info = [
            'fertilapplicationinfo',
            'fertilizerinfo',
            'harvestinfo',
            'measurements',
            'sowinginfo',
            'yieldinfo',
            'plantheight',
            ]


sentinel1 = [
            'sentinel1data',
            'datacatalogue',
            's1demzstatistic',
            's1fieldstatistic',
            's1maskvegparameters',
            's1rowdirectionaspectangle',
            's1rowdirectionzstatistic',
            's1soilzstatistic',
            's1vegparameters',
            's1yldzstatistic',
            'sarparamreclass',
            'sentinel1data',
            ]

sentinel2 = [
            'sentinel2cloudcover',
            'sentinel2data',
            'sentinel2metaonline',
            's2vegparameters'
            ]

uav_data = [
            'uav_datacatalogue',
            'uav_metadata',
            'uavcnnaccuracy',
            'uavcnnaccuracyreport',
            'uavfeaturejmd',
            'uavfeaturestatistic',
            'uavfimportance',
            'uavmembershipf',
            'uavpattern',
            'uavsemivariogram',
            'uavsemivarmodelperfom', # uavsemivarmodelperform  # Change to it !!!!
            ]




# Define groups of tables and assign colors
groups = {
    "Group5": sentinel2,
    "Group1": study_area,
    "Group4": sentinel1,
    "Group3": managment_info,
    #"Group4": sentinel1,
    #"Group5": sentinel2,
    "Group6": uav_data,
    "Group2": dwd_data,
}



# Map tables to colors, ensuring the order is consistent with groups
group_colors = {}
for group, tables in groups.items():
    color = {"Group1": "#E6E6FA", "Group2": "#FFDAB9", "Group3": "#00CC99", "Group4": "#ADD8E6", "Group5": "#98FB98" , "Group6": "#FFB6C1"}[group]
    group_colors.update({table: color for table in tables})



db_path = r'...\RCM.db'
dbarchive = connect2db(db_path)  # Connect to your database
conn = dbarchive.archive.conn



excluded_prefixes = ['geometry', 'idx', 'virts', 'views', 'spatial', 'report', 'sql', 'lost']

# this function also will exclude the tables that are empty in the DB
relationships_df, primary_keys = get_table_relationships(db_path, conn, excluded_prefixes)
plot_relationship_graph(relationships_df,
                        primary_keys,
                        group_colors,
                        label_font_size=9,
                        pk_font_size=7,
                        figsize=(14, 14),
                        layout_type='shell',
                        save_to=r'...\Antrag\REPORTS\REPORT_2024\imgs\DB\RCM_db_relationship_graph2.png'
                        )