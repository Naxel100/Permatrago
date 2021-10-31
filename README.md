Permatrago
==============================

# Causal LM for commit messages

### Model

The model chosen for doing the suggestions of text in the commit messages has been a deep neural network based on decoder transformers. In this case we have fine-tuned DistilGPT2 which is a lighter but faster version of OpensAI’s GPT2 developed by huggingface trained with a reproduction of OpenAI's WebText dataset (https://skylion007.github.io/OpenWebTextCorpus/). The model has 6 layers, 768 dimensions and 12 heads, totalizing 82M parameters

<p align="center">
  <img src='README images/model.png'/ width = 250>
</p>

However, we must consider that the network is not able neither to process the raw text as input nor to generate raw text at the output. In order to be able to forward the text through the network and to generate a comprehensible output the raw text used either in the training phase or the inference phase must be translated to a numeric based vocabulary. For this purpose, the creators of the model architecture provide with it a tokenizer so we can convert the raw text input to a numeric system comprehensible for the network and can convert the output back to text. 

<p align="center">
  <img src='README images/token.png'/ width = 250>
</p>

### Installation and execution

<p align="center">
  <img src='README images/demo.png'/ width = 250>
</p>


Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------
## Team

This project was developed by:
| [![CarlOwOs](https://avatars.githubusercontent.com/u/49389491?v=4)](https://github.com/CarlOwOs) | [![Naxel100](https://avatars2.githubusercontent.com/u/43076234?v=4)](https://github.com/Naxel100) | [![marcfuon](https://avatars2.githubusercontent.com/u/49389563?v=4)](https://github.com/marcfuon) | [![turcharnau](https://avatars.githubusercontent.com/u/70148725?v=4)](https://github.com/turcharnau) |
| --- | --- | --- | --- |
| [Carlos Hurtado](https://github.com/CarlOwOs) | [Alex Ferrando](https://github.com/Naxel100) | [Marc Fuentes](https://github.com/marcfuon) | [Arnau Turch](https://github.com/turcharnau) |


Students of Data Science and Engineering at [UPC](https://www.upc.edu/ca).
