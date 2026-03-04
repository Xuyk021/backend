import os

DATA_PATH = r"C:/PSU/FSU/Data"

PAPERS = os.path.join(DATA_PATH, "SciSciNet_Papers.tsv")
AUTHORS = os.path.join(DATA_PATH, "SciSciNet_Authors.tsv")
PAPER_AUTHORS = os.path.join(
    DATA_PATH,
    "SciSciNet_PaperAuthorAffiliations.tsv"
)
REFERENCES = os.path.join(
    DATA_PATH,
    "SciSciNet_PaperReferences.tsv"
)
PAPER_FIELDS = os.path.join(DATA_PATH, "SciSciNet_PaperFields.tsv")
FIELDS = os.path.join(DATA_PATH, "SciSciNet_Fields.tsv")
AFFILIATIONS = os.path.join(DATA_PATH, "SciSciNet_Affiliations.tsv")
PATENTS = os.path.join(DATA_PATH, "SciSciNet_Link_Patents.tsv")