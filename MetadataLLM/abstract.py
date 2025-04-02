from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import os
from abc import ABC, abstractmethod


class ModelManager(ABC):
    # Atributos de clase para el modelo y el tokenizador
    tokenizer = None
    model = None
    temperature = 0.75
    top_p = 0.8
    repetition_penalty = 1.2

    @classmethod
    def initialize_model(cls, model_name, access_token, device="cuda"):
        os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
        torch.device(device)
        cls.tokenizer = AutoTokenizer.from_pretrained(model_name, token=access_token)
        cls.model = AutoModelForCausalLM.from_pretrained(
            model_name, token=access_token
        ).to(device)
        cls.temperature = 0.75
        cls.top_p = 0.8
        cls.repetition_penalty = 1.2

    def __init__(self, device="cuda"):
        self.device = device

    @abstractmethod
    def logger_tag(self):
        pass
