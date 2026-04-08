import matplotlib.pyplot as plt


def show_bar_chart(pdf):
    pdf.plot(kind='bar', figsize=(10, 6))

    plt.title("Score Comparison")
    plt.ylabel("Scores")
    plt.xticks(rotation=45)
    plt.legend(title="Dataset")
    plt.tight_layout()

    plt.show()