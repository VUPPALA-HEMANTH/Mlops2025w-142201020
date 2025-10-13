import wandb
from datasets import load_dataset
from collections import Counter
import matplotlib.pyplot as plt

# Load dataset (requires trusting the remote dataset script on the Hugging Face Hub)
# dataset = load_dataset("eriktks/conll2003", trust_remote_code=True)
# dataset = load_dataset("conll2003")
# dataset = load_dataset("conll2003", download_mode="force_redownload")
from conll2003 import Conll2003

dataset_builder = Conll2003()
dataset_builder.download_and_prepare()
dataset = dataset_builder.as_dataset()



# Extract ner_tags feature info
ner_tag_features = dataset["train"].features["ner_tags"].feature
ner_config = {
    "names": ner_tag_features.names,
    "num_classes": ner_tag_features.num_classes,
}

# run-1 for initial, 
# run-2 for data distribution plot
# run-3 for entity distribution plots
# Initialize W&B run with name and config
wandb.init(
    project="Q1-weak-supervision-ner",
    name="run-3", # for plots, run-1 for initial
    config={
        "dataset": "CoNLL-2003",
        "splits": list(dataset.keys()),
        "ner_tags": ner_config
    }
)

# Count total number of samples
num_train = len(dataset["train"])
num_val   = len(dataset["validation"])
num_test  = len(dataset["test"])

# Count entity distribution
def get_entity_counts(split):
    entity_counter = Counter()
    for tokens, labels in zip(split["tokens"], split["ner_tags"]):
        for label_id in labels:
            # HuggingFace provides label IDs; map to label names
            label_name = dataset["train"].features["ner_tags"].feature.int2str(label_id)
            entity_counter[label_name] += 1
    return entity_counter

train_entity_counts = get_entity_counts(dataset["train"])
val_entity_counts   = get_entity_counts(dataset["validation"])
test_entity_counts  = get_entity_counts(dataset["test"])

# Log dataset statistics to W&B
wandb.summary["num_train_samples"] = num_train
wandb.summary["num_val_samples"]   = num_val
wandb.summary["num_test_samples"]  = num_test

# Log entity distributions as dictionaries
wandb.summary["train_entity_counts"] = dict(train_entity_counts)
wandb.summary["val_entity_counts"]   = dict(val_entity_counts)
wandb.summary["test_entity_counts"]  = dict(test_entity_counts)

print("Dataset statistics logged to W&B!")


# Number of samples
split_sizes = [num_train, num_val, num_test]
split_labels = ["Train", "Validation", "Test"]

# Create pie chart
plt.figure(figsize=(6,6))
plt.pie(split_sizes, labels=split_labels, autopct="%1.1f%%", startangle=90, colors=["skyblue","lightgreen","salmon"])
plt.title("Dataset Split Proportions")
plt.tight_layout()
plt.savefig("plots/dataset_split_pie.png")
plt.close()
# plt.show()
wandb.log({"Dataset Split Proportions": wandb.Image("plots/dataset_split_pie.png")})


# Example: training entity distribution
train_labels = list(train_entity_counts.keys())
train_counts = list(train_entity_counts.values())

val_labels = list(val_entity_counts.keys())
val_counts = list(val_entity_counts.values())

test_labels = list(test_entity_counts.keys())
test_counts = list(test_entity_counts.values())

def plot_entity_distribution(labels, counts, title):
    plt.figure(figsize=(8,5))
    plt.bar(labels, counts, color='skyblue')
    plt.xticks(rotation=45)
    plt.ylabel("Number of tokens")
    plt.title(title)
    plt.tight_layout()
    return plt

train_plot = plot_entity_distribution(train_labels, train_counts, "Train Entity Distribution")
val_plot = plot_entity_distribution(val_labels, val_counts, "Validation Entity Distribution")
test_plot = plot_entity_distribution(test_labels, test_counts, "Test Entity Distribution")
train_plot.savefig("plots/train_entity_distribution.png")
val_plot.savefig("plots/val_entity_distribution.png")
test_plot.savefig("plots/test_entity_distribution.png")

# Log images to W&B
wandb.log({
    "Train Entity Distribution": wandb.Image("plots/train_entity_distribution.png"),
    "Validation Entity Distribution": wandb.Image("plots/val_entity_distribution.png"),
    "Test Entity Distribution": wandb.Image("plots/test_entity_distribution.png")
})


# Finish W&B run
wandb.finish()