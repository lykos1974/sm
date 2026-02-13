import re
import unicodedata

SYNONYMS = {
    "tomata": "tomata",
    "domata": "tomata",
    "ntomata": "tomata",
    "ntomates": "tomata",
    "tomatoes": "tomata",
    "γαλα": "milk",
    "gala": "milk",
    "milk": "milk",
    "psomi": "bread",
    "pswmi": "bread",
    "bread": "bread",
    "ρυζι": "rice",
    "ryzi": "rice",
    "rizi": "rice",
    "rice": "rice",
}

STOPWORDS = {
    "fresh", "full", "plires", "xama", "elliniki", "to", "του", "χυμα", "greek",
    "kg", "gr", "g", "lt", "l", "litro", "liter", "litre", "ml", "τεμ",
}

GREEK_LATIN_MAP = {
    "α": "a", "β": "v", "γ": "g", "δ": "d", "ε": "e", "ζ": "z", "η": "i",
    "θ": "th", "ι": "i", "κ": "k", "λ": "l", "μ": "m", "ν": "n", "ξ": "x",
    "ο": "o", "π": "p", "ρ": "r", "σ": "s", "ς": "s", "τ": "t", "υ": "y",
    "φ": "f", "χ": "ch", "ψ": "ps", "ω": "o",
}


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def transliterate_greek(text: str) -> str:
    return "".join(GREEK_LATIN_MAP.get(ch, ch) for ch in text)


def normalize_text(text: str) -> str:
    text = text.lower().strip()
    text = strip_accents(text)
    text = transliterate_greek(text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_token(token: str) -> str:
    token = token.strip()
    if re.fullmatch(r"\d+(kg|g|gr|ml|l|lt)?", token):
        return ""
    if token.endswith(("es", "s")) and len(token) > 4:
        token = token[:-1]
    if "tomat" in token or "ntomat" in token:
        return "tomata"
    if token.startswith("gal") or token.startswith("mil"):
        return "milk"
    if token.startswith("ryz") or token.startswith("riz") or token.startswith("ric"):
        return "rice"
    if token.startswith("psom") or token.startswith("brea"):
        return "bread"
    token = SYNONYMS.get(token, token)
    if token in STOPWORDS:
        return ""
    return token


def canonicalize_tokens(text: str) -> list[str]:
    tokens = normalize_text(text).split()
    canonical = [_normalize_token(token) for token in tokens if token]
    canonical = [token for token in canonical if token]
    return sorted(canonical)


def match_score(a: str, b: str) -> float:
    a_tokens = set(canonicalize_tokens(a))
    b_tokens = set(canonicalize_tokens(b))
    if not a_tokens or not b_tokens:
        return 0.0

    overlap = len(a_tokens & b_tokens)
    coverage = overlap / max(1, len(a_tokens))
    jaccard = overlap / len(a_tokens | b_tokens)
    return max(coverage, (coverage * 0.7 + jaccard * 0.3))


def is_same_item(a: str, b: str, threshold: float = 0.65) -> bool:
    return match_score(a, b) >= threshold
