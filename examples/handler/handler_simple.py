from pydantic import BaseModel

from tadween_core.handler import BaseHandler


# 1. Define Input and Output schemas
class TextAnalysisInput(BaseModel):
    text: str
    language: str = "en"


class TextAnalysisOutput(BaseModel):
    sentiment: str
    confidence: float
    analysis_duration: float


# 2. Implement a simple handler
class TextAnalysisHandler(BaseHandler[TextAnalysisInput, TextAnalysisOutput]):
    def warmup(self):
        print("[Handler] Warming up TextAnalysisHandler...")
        # Simulating model loading
        pass

    def run(self, inputs: TextAnalysisInput) -> TextAnalysisOutput:
        print(f"[Handler] Running analysis for language '{inputs.language}':")
        print(f"           Text: {inputs.text}")

        # Simulating analysis logic
        sentiment = "positive" if "happy" in inputs.text.lower() else "neutral"

        return TextAnalysisOutput(
            sentiment=sentiment, confidence=0.95, analysis_duration=0.1
        )

    def shutdown(self):
        print("[Handler] Shutting down TextAnalysisHandler...")
        pass


def main():
    handler = TextAnalysisHandler()
    handler.warmup()

    # 4. Execute the handler logic
    inputs = TextAnalysisInput(text="I am very happy today!", language="en")
    print("[Client] Submitting text for analysis...")
    output = handler.run(inputs)

    # 5. Process the result
    print("[Client] Analysis complete!")
    print(f"          Sentiment: {output.sentiment}")
    print(f"          Confidence: {output.confidence}")
    print(f"          Duration: {output.analysis_duration}s")

    # 6. Shutdown the handler
    handler.shutdown()


if __name__ == "__main__":
    main()
