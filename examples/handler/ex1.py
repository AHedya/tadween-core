from pydantic import BaseModel

from tadween_core.handler import BaseHandler


class TextAnalysisInput(BaseModel):
    text: str
    language: str = "en"


class TextAnalysisOutput(BaseModel):
    sentiment: str
    confidence: float
    analysis_duration: float


class TextAnalysisHandler(BaseHandler[TextAnalysisInput, TextAnalysisOutput]):
    def warmup(self):
        import random

        # simulate loading model
        self.random_module = random

        print("[Handler] Warming up TextAnalysisHandler...")

    def run(self, inputs: TextAnalysisInput) -> TextAnalysisOutput:
        sentiment = "positive" if "happy" in inputs.text.lower() else "neutral"

        return TextAnalysisOutput(
            sentiment=sentiment,
            # same as import random; random.random()
            confidence=round(self.random_module.random(), 2),
            analysis_duration=0.1,
        )

    def shutdown(self):
        print("[Handler] Shutting down TextAnalysisHandler...")
        pass


def main():
    handler = TextAnalysisHandler()
    handler.warmup()

    inputs = TextAnalysisInput(text="I am very happy today!", language="en")
    print("[Client] Submitting text for analysis...")
    output = handler.run(inputs)

    print("[Client] Analysis complete!")
    print(f"\tSentiment: {output.sentiment}")
    print(f"\tConfidence: {output.confidence}")
    print(f"\tDuration: {output.analysis_duration}s")

    handler.shutdown()


if __name__ == "__main__":
    main()
