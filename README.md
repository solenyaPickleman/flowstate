# flowstate

Flowstate consists of an original flow which parses the plaintext email files with Flanker and spark and inserts them into Postgres, and a few other scripts which train and/or apply ml models to enhance the collection. Results are stored in Postgres ( per sql_definition.sql ), with the idea being that a join can bring together different analysis, done at different times, into a coherant view. The analytics so far are : 

  - NER extraction of entities ( PERSON, ORG, LOCATION) via xlm_roberta_large_token_classifier_hrl_pipeline (add_entities.py ) 
  - Language detection with cld3  ( add_language.py ) 
  - A spam/ham flag, based on a custom UniversalSentenceEncoder->ClassifierDL model and separate training dataset (training/train_spamham_classifier.py) 
