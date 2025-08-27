# nlp/aspects.py
# Dependency-free, fast aspect tagger.
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Dict, List

LEXICON: Dict[str, List[str]] = {
    "battery": [
        r"battery(?:\s+life)?", r"\bcharge\b", r"charging", r"drain(?:ing)?",
        r"screen[\s-]on[\s-]time", r"\bSOT\b", r"power[\s-]?saving",
        r"battery\s*health", r"overnight\s*drain", r"\bpower\b", r"heat(?:ing)?", r"thermals?"
    ],
    "camera": [
        r"\bcamera\b", r"\bphoto(s)?\b", r"\bvideo(s)?\b", r"\bHDR\b", r"portrait",
        r"bokeh", r"ultra[\s-]?wide", r"tele(photo)?", r"\bzoom\b",
        r"(?:stabili[sz]ation|OIS|EIS)", r"selfie", r"night\s*mode",
        r"low[\s-]?light", r"skin\s*tones?", r"shutter\s*lag"
    ],
    "screen": [
        r"\bscreen\b", r"\bdisplay\b", r"(?:OLED|AMOLED|LCD)\b",
        r"brightness|nits|too\s*dim|too\s*bright",
        r"refresh\s*rate|\b\d{2,3}hz\b", r"PWM|flicker", r"resolution", r"scratch(?:es)?"
    ],
    "shipping": [
        r"shipping", r"deliver(?:y|ed)|delay(?:s)?|late|on\s*time",
        r"package|packaging|box", r"courier|logistics", r"fedex|ups|dhl|usps",
        r"return|refund|rma|replacement"
    ],
    "price": [
        r"\bprice\b|\bpricing\b", r"\bcost\b|\bmsrp\b", r"expensive|overpriced",
        r"cheap|affordable", r"value\s*for\s*money|worth\s*it|deal|discount|offer"
    ],
}

def _compile(p: str) -> re.Pattern:
    if p.startswith(r"\b") or p.endswith(r"\b"):
        return re.compile(p, re.I)
    return re.compile(rf"\b(?:{p})\b", re.I)

COMPILED = {a: [_compile(p) for p in pats] for a, pats in LEXICON.items()}

@dataclass
class TagResult:
    labels: List[str]
    scores: Dict[str, int]

class AspectTagger:
    def __init__(self, top_k: int | None = None, min_hits: int = 1):
        self.top_k = top_k
        self.min_hits = max(1, int(min_hits))

    def tag(self, text: str) -> TagResult:
        if not text:
            return TagResult([], {})
        hits = []
        for aspect, patterns in COMPILED.items():
            cnt = sum(1 for rx in patterns for _ in rx.finditer(text))
            if cnt >= self.min_hits:
                hits.append((aspect, cnt))
        hits.sort(key=lambda t: (-t[1], t[0]))
        if self.top_k is not None:
            hits = hits[: self.top_k]
        return TagResult([a for a,_ in hits], {a:c for a,c in hits})
