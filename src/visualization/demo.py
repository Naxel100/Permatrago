import streamlit as st
import torch
import time
import numpy as np

from src.utils.misc import load_model, next_words

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model_path = '../../models/EL_MODELO'

@st.cache(allow_output_mutation=True)
generator = load_model(device, model_path)

sentence = ""

st.title('Text prediction 🤖🔮')
col1, col2 = st.columns(2)
with col1:
    added = st.number_input('Rows added', min_value=0)
    removed = st.number_input('Rows removed',min_value=0)
with col2:
    seed = st.number_input('Seet a seed',min_value=0, value = 123)
    n_words = st.number_input('Number of suggested words',min_value=1)

sentence = st.text_input('Insert commit message') 

if sentence or added or removed:
    if sentence:
        def_sentence = str(added)+' '+ str(removed) +' '+sentence
    else:
        def_sentence = str(added)+' '+ str(removed)
    res = next_words(def_sentence, generator, n_words = n_words ,seed=seed)
    
    res = list(res)
    num_out = len(res)
    cols = st.columns(num_out)
    for i, col in enumerate(cols):
        col.write(res[i])



