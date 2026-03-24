import asyncio
import json
import random
from typing import Generic, TypeVar

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import BaseModel, ValidationError

from crowdgit.logger import logger
from crowdgit.settings import (
    CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
    CROWD_AWS_BEDROCK_REGION,
    CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
)

T = TypeVar("T", bound=BaseModel)


class BedrockResponse(BaseModel, Generic[T]):
    output: T
    cost: float


MAX_THROTTLE_RETRIES = 5
THROTTLE_BASE_DELAY = 10  # seconds


async def invoke_bedrock(
    instruction, pydantic_model: type[T], replacements=None, max_tokens=65000, temperature=0
) -> BedrockResponse[T]:
    session = aioboto3.Session(
        aws_access_key_id=CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
        aws_secret_access_key=CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
    )

    async with session.client(
        service_name="bedrock-runtime",
        region_name=CROWD_AWS_BEDROCK_REGION,
        config=Config(
            read_timeout=300,  # 5 minutes timeout for reading response
            connect_timeout=60,  # 1 minute timeout for connection
            retries={"max_attempts": 3},  # Retry failed requests
        ),
    ) as bedrock_client:
        # Apply replacements to the instruction string
        if replacements:
            for key, value in replacements.items():
                if not key.startswith("{") and not key.endswith("}"):
                    key = "{" + key + "}"
                if key in instruction:
                    if isinstance(value, list):
                        value = ", ".join(map(str, value))
                    elif isinstance(value, dict):
                        value = json.dumps(value)
                    instruction = instruction.replace(key, value)
                else:
                    raise ValueError(f"Replacement key '{key}' not found in the instruction")

        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "system": "You are a precise JSON extraction assistant. You MUST respond with valid JSON only. Never use markdown formatting, code blocks, or any additional text. Your response must be parseable by json.loads() directly.",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": instruction,
                            },
                        ],
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": temperature,
            }
        )

        modelId = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        accept = "application/json"
        contentType = "application/json"

        for attempt in range(1, MAX_THROTTLE_RETRIES + 1):
            try:
                response = await bedrock_client.invoke_model(
                    body=body, modelId=modelId, accept=accept, contentType=contentType
                )
                break
            except ClientError as e:
                if (
                    e.response["Error"]["Code"] == "ThrottlingException"
                    and attempt < MAX_THROTTLE_RETRIES
                ):
                    delay = THROTTLE_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 2)
                    logger.warning(
                        f"Bedrock ThrottlingException (attempt {attempt}/{MAX_THROTTLE_RETRIES}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

        try:
            body_bytes = await response["body"].read()
            response_body = json.loads(body_bytes.decode("utf-8"))
            raw_text = response_body["content"][0]["text"].replace('"""', "").strip()

            # Strip markdown code fences if present (Haiku sometimes ignores the system prompt)
            if raw_text.startswith("```"):
                raw_text = raw_text.split("\n", 1)[-1]
                if raw_text.endswith("```"):
                    raw_text = raw_text.rsplit("```", 1)[0]
                raw_text = raw_text.strip()

            output = json.loads(raw_text)

            # Calculate cost (Claude Haiku 4.5 on AWS Bedrock: $1.00/$5.00 per 1M tokens)
            input_tokens = response_body["usage"]["input_tokens"]
            output_tokens = response_body["usage"]["output_tokens"]
            input_cost = (input_tokens / 1_000_000) * 1.00
            output_cost = (output_tokens / 1_000_000) * 5.00
            total_cost = input_cost + output_cost

            # Validate output with the provided model if it exists
            try:
                validated_output = pydantic_model.model_validate(output, strict=True)
            except ValidationError as ve:
                logger.error(f"Output validation failed: {ve}")
                raise ve

            return BedrockResponse[T](output=validated_output, cost=total_cost)
        except Exception as e:
            logger.error("Failed to parse the response as JSON. Raw response:")
            logger.error(response_body["content"][0]["text"])
            raise e
