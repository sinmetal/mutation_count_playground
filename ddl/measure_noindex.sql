CREATE TABLE MeasureNoIndex (
    ID STRING(MAX) NOT NULL,
    Arr1 ARRAY<STRING(MAX)>,
    Col1 STRING(MAX),
    Col2 STRING(MAX),
    Col3 STRING(MAX),
    Col4 STRING(MAX),
    Col5 STRING(MAX),
    Col6 STRING(MAX),
    Col7 STRING(MAX),
    Col8 STRING(MAX),
    Col9 STRING(MAX),
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);