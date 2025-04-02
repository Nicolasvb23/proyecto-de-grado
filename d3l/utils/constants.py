import re
import unicodedata

from nltk.corpus import stopwords

ADDITSTOPWORDS = ["yes", "no", "si"]
STOPWORDS = stopwords.words("english") + stopwords.words("spanish") + ADDITSTOPWORDS
MISSING_VALUES = [
    "N/C",
    "NO CLASIFICADO",
    "NO CONSTA",
    "NA",
    "N/A",
    "ND",
    "NS",
    "NR",
    "SIN INFORMACIÓN",
    "SIN REGISTRO",
    "DESCONOCIDO",
    "PENDIENTE",
    "---",
    "...",
    "???",
    "",
    "NONE",
    "NULL",
    "NAN",
    "-1",
    "999",
    "S/N",
    "S/R",
    "S/I",
    "NC",
]


# Currency symbols ($)
SYMBPATT = r"\@" + re.escape(
    "".join(chr(i) for i in range(0xFFFF) if unicodedata.category(chr(i)) == "Sc")
)
PUNCTPATT = r"\!\"\#\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\[\\\]\^\_\`\{\|\}\~"

ALPHANUM = re.compile(r"(?:[0-9]+[a-zA-Z]|[a-zA-Z]+[0-9])[a-zA-Z0-9]*")

POSDEC = re.compile(r"\+?[0-9]+(,[0-9]+)*\.[0-9]+")
NEGDEC = re.compile(r"\-[0-9]+(,[0-9]+)*\.[0-9]+")
POSINT = re.compile(r"\+?[0-9]+(,[0-9]+)*")
NEGINT = re.compile(r"\-[0-9]+(,[0-9]+)*")

NUMSYMB = re.compile(
    r"(?=.*[0-9,\.])(?=.*[" + SYMBPATT + r"]+)([0-9" + SYMBPATT + r"]+)", re.UNICODE
)

LOWALPHA = re.compile(r"[a-z]([a-z\-])*")
UPPALPHA = re.compile(r"[A-Z]([A-Z\-\.])*")
CAPALPHA = re.compile(r"[A-Z][a-z]([a-z\-])*")

PUNCT = re.compile(r"[" + PUNCTPATT + r"]+")
SYMB = re.compile(r"[" + SYMBPATT + r"]+", re.UNICODE)
WHITE = re.compile(r"\s+")

FASTTEXTURL = "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/"
GLOVEURL = "http://nlp.stanford.edu/data/"
