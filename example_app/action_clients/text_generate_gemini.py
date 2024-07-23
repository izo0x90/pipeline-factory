import logging
import pathlib
from typing import Optional

import google.generativeai as genai
from google.generativeai.types import HarmBlockThreshold, HarmCategory


logger = logging.getLogger(__name__)


MAX_RETRIES = 3

RESPONSE_DELIMITER = "Speech:"

GENERIC_PROMPT = (
    "You are the historic figure by the name of {person_full_name}.\n"
    "Find a few quotes from the above historic figure. Use those quotes to generate a speech about {subject} ."
    "This speech should be in the tone and style of the historic figure, and be rooted in their historic experience.\n"
    f'Indicate where the speech begins using "{RESPONSE_DELIMITER}" heading.\n'
    "Example:\n\n"
    "``` "
    '"Quote 1" - Author\n'
    '"Quote 2" - Author\n'
    f"{RESPONSE_DELIMITER}"
    "This is an example deeply spiritual speech text!"
    " ```"
)

# TODO: (Hristo) - 429 RESOURCE_EXHAUSTED, should it be handled (Potential)


class PromptBuilder:
    def __call__(self, prompt_template: str, **kwargs) -> str:
        prompt = prompt_template
        for name, text in kwargs.items():
            prompt = prompt.replace("{" + name + "}", text)
        if not prompt:
            raise ValueError("No replacements passed in")

        return prompt

    @staticmethod
    def template_from_file(template_path: pathlib.Path):
        with open(template_path, "r") as f:
            return f.read()


class QueryVertexModel:
    def __init__(self, key: str, prompt=None, result_delimiter: Optional[str] = None):
        genai.configure(api_key=key)
        self.prompt = prompt
        self.delimiter = result_delimiter
        self.last_result = None
        self.model = genai.GenerativeModel("gemini-1.0-pro-latest")
        self.model_params = genai.types.GenerationConfig(
            # Only one candidate for now.
            candidate_count=1,
            # stop_sequences=['x'],
            # max_output_tokens=20,
            temperature=1.0,
        )

    def execute_prompt(self, prompt):
        return self.model.generate_content(
            prompt,
            generation_config=self.model_params,
            safety_settings={
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            },
        )

    def extract_text(self, content_response):
        logger.debug(f"Raw LLM response: {content_response=}")
        try:
            if self.delimiter:
                _, content = content_response.text.split(self.delimiter)
                _, content = content.split("\n", 1)
            else:
                content = content_response.text
        except ValueError:
            logger.debug("Failed generation")
            content = None

        return content

    def get_text(self, retry=True):
        text = None
        for _ in range(MAX_RETRIES):
            text = self.extract_text(self.execute_prompt(self.prompt))

            if text or not retry:
                break

        self.last_result = text
        return text
