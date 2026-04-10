import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

from util.logger import log_message


#           SAMPLE INPUT
# data = [
#     ['analysis', 'data quality', 'remainingenrichment.csv', 0.7081],
#     ['analysis', 'data quality', 'vnm_bag_customer_match.csv', 0.8646],
#     ['archive', 'accuracy scoring', 'address_92k.csv', 71.2663]
# ]

def show_bar_chart(config_info,data):

    chart_filename = config_info["paths"]["output_graph_file"]

    log_message("Plot chart processing start")

    # convert list to data frame
    temp_df = pd.DataFrame(data,columns=["database","metric","filename","score"])

    # group df on database and metric
    plot_df = temp_df.groupby(["database","metric"])["score"].mean().reset_index()

    # print(plot_df)

    # convert data in long format to wide format. index = row fields, columns = column fields 
    pivot_df = plot_df.pivot(index='database', columns='metric', values='score')
    # print(pivot_df)

    pivot_df.plot(kind="bar",figsize=(12,6))

    plt.title("Database vs Metrics")
    plt.xlabel("Database")
    plt.ylabel("Metrics")

    # rotate labels to adjust long labels on x-axis to show without overlap
    plt.xticks(rotation=45) 
    plt.legend(title="Metrics")

    plt.savefig(chart_filename)

    plt.tight_layout()
    plt.show()

    log_message("Plot chart processing end")

