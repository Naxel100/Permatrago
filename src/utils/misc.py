import torch
import transformers
from transformers import AutoModelForCausalLM
from transformers import AutoTokenizer
from transformers import pipeline, set_seed

def load_model(device, model_path):
    model_checkpoint = "distilgpt2" 
    model = AutoModelForCausalLM.from_pretrained(model_checkpoint)

    tokenizer = AutoTokenizer.from_pretrained(model_checkpoint, use_fast=True)

    cp = torch.load( model_path, map_location=torch.device(device))
    model.load_state_dict(cp)
    model.eval()

    generator = pipeline('text-generation', model=model, tokenizer=tokenizer)

    return generator

def next_words(text, generator, n_words=2, seed=None, ):

    if seed:
      set_seed(seed)

    next_words=set()

    output=generator(text, max_length=len(text)+3*n_words, num_return_sequences=3)

    for out in output:
      next_words.add(' '.join(out['generated_text'].replace(text,"").split()[0:n_words]))

    return next_words