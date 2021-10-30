torch.manual_seed(1234)
import transformers
import pandas as pd
from datasets import load_dataset, Dataset
dataset = load_dataset('csv', data_files='/../../data/processed/DATA_PRE1.csv')

words = {}
for idx, row in data.iterrows():
    sentence = str(row.COMMIT_MESSAGE).split(' ')
    for w in sentence:
        words[w] = 1;

data['final_message']= data['LINES_ADDED'].astype(str)+' '+data['LINES_REMOVED'].astype(str)+' '+data['COMMIT_MESSAGE']
data_no_nulls = data.dropna()
dataset = Dataset.from_pandas(data_no_nulls)

from transformers import AutoTokenizer
model_checkpoint = "distilgpt2"
tokenizer = AutoTokenizer.from_pretrained(model_checkpoint, use_fast=True)

def tokenize_function(examples):
    return tokenizer(examples['final_message'])

tokenizer.model_max_length=5012

tokenized_dataset = dataset.map(tokenize_function, batched=True, num_proc=1, remove_columns=["COMMIT_MESSAGE","COMMIT_HASH","PARENTS","FILE","LINES_ADDED","LINES_REMOVED","final_message","__index_level_0__"])

from datasets import ClassLabel
import random
from IPython.display import display, HTML

def show_random_elements(dataset, num_examples=10):
    assert num_examples <= len(dataset), "Can't pick more elements than there are in the dataset."
    picks = []
    for _ in range(num_examples):
        pick = random.randint(0, len(dataset)-1)
        while pick in picks:
            pick = random.randint(0, len(dataset)-1)
        picks.append(pick)

    df = pd.DataFrame(dataset[picks])
    for column, typ in dataset.features.items():
        if isinstance(typ, ClassLabel):
            df[column] = df[column].transform(lambda i: typ.names[i])
    display(HTML(df.to_html()))

block_size = 128

def group_texts(examples):
    # Concatenate all texts.
    concatenated_examples = {k: sum(examples[k], []) for k in examples.keys()}
    total_length = len(concatenated_examples[list(examples.keys())[0]])
    # We drop the small remainder, we could add padding if the model supported it instead of this drop, you can
        # customize this part to your needs.
    total_length = (total_length // block_size) * block_size
    # Split by chunks of max_len.
    result = {
        k: [t[i : i + block_size] for i in range(0, total_length, block_size)]
        for k, t in concatenated_examples.items()
    }
    result["labels"] = result["input_ids"].copy()
    return result

lm_datasets = tokenized_dataset.map(
    group_texts,
    batched=True,
    batch_size=1000,
    num_proc=4,
)

from transformers import AutoModelForCausalLM
model = AutoModelForCausalLM.from_pretrained(model_checkpoint)

from transformers import Trainer, TrainingArguments

model_name = model_checkpoint.split("/")[-1]
training_args = TrainingArguments(
    f"{model_name}-finetuned-commits",
    evaluation_strategy = "epoch",
    learning_rate=2e-5,
    weight_decay=0.01,
    push_to_hub=False,
)

from datasets import DatasetDict

train_valid = lm_datasets.train_test_split(0.2, seed=1234)
# gather everyone if you want to have a single DatasetDict
final_dataset = DatasetDict({
    'train': train_valid['train'],
    'valid': train_valid['test']})

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=final_dataset["train"],
    eval_dataset=final_dataset["valid"],
)

import torch
trainer.train()

import math
eval_results = trainer.evaluate()
print(f"Perplexity: {math.exp(eval_results['eval_loss']):.2f}")

torch.save(model.state_dict(), '/../../models/EL_MODELO')
