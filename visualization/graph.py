import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def result_graph():
    df = pd.read_csv("results.csv")
    df.set_index('Unnamed: 0', inplace=True)

    df = df.transpose()
    x = np.arange(0,6)  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0
    species = ("name", "name \n+ evidence", "type",
               "type\n+ evidence", "description",
               "description\n+ evidence")
    penguin_means = {
        'recall': df['result_recall_corrected'],
        'precision': df['result_precision_corrected'],
    }

    fig, ax = plt.subplots(layout='constrained')

    for attribute, measurement in penguin_means.items():
        if attribute == 'jaccard':c = 'yellowgreen'
        if attribute == 'recall':c = 'darkslateblue'
        if attribute == 'precision':c = 'mediumaquamarine'
        offset = width * multiplier
        ax.bar(x + offset, measurement, width, label=attribute,color=c)
        multiplier += 1

    ax.set_xticks(x + width, species)
    ax.legend(loc='upper left', ncols=3)
    ax.set_ylim(0, 0.3)

    #plt.show()
    plt.savefig("plots/results_graph.svg")

def plan_graph():
    df = pd.read_csv("results.csv")
    df.set_index('Unnamed: 0', inplace=True)

    df = df.transpose()
    x = np.arange(0,6)  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0
    species = ("name", "name \n+ evidence", "type",
               "type\n+ evidence", "description",
               "description\n+ evidence")
    penguin_means = {
        'recall': df['recall_corrected'],
        'precision': df['precision_corrected'],
        'jaccard': df['jaccard_corrected'],
    }

    fig, ax = plt.subplots(layout='constrained')

    for attribute, measurement in penguin_means.items():
        if attribute == 'jaccard':c = 'yellowgreen'
        if attribute == 'recall':c = 'darkslateblue'
        if attribute == 'precision':c = 'mediumaquamarine'
        offset = width * multiplier
        ax.bar(x + offset, measurement, width, label=attribute,color=c)
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xticks(x + width, species)
    ax.legend(loc='upper left', ncols=3)
    ax.set_ylim(0, 0.7)

    #plt.show()
    plt.savefig("plots/plan_graph.svg")

def iteration_plan():
    df = pd.read_csv("iteration_correction.csv")

    x = np.arange(0,6,1)
    y = df[["planRecall","PlanPRecision","JAccard"]].to_numpy()
    colors = ['darkslateblue','mediumaquamarine','yellowgreen']
    labels = ['recall', 'precision', 'jaccard']
    fig, ax = plt.subplots()
    for i in range(3):
        ax.plot(x, y[:,i],'x-', label=labels[i], color=colors[i])
    ax.legend(['recall', 'precision', 'jaccard'])

    plt.grid(axis='both', color='0.95')
    plt.tight_layout()

    plt.savefig("plots/plan_iteration.svg")

def iteration_results():
    df = pd.read_csv("iteration_correction.csv")

    x = np.arange(0,6,1)
    y = df[["Recall","Precision"]].to_numpy()
    colors = ['darkslateblue','mediumaquamarine']
    labels = ['recall', 'precision']
    fig, ax = plt.subplots()
    for i in range(2):
        ax.plot(x, y[:,i],'x-', label=labels[i], color=colors[i])
    ax.legend(['recall', 'precision'], loc=4)

    plt.grid(axis='both', color='0.95')
    plt.tight_layout()
    #plt.show()
    plt.savefig("plots/result_iteration.svg")

if __name__ == "__main__":
    result_graph()