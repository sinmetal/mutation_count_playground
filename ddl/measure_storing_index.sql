CREATE TABLE MeasureWithStoring (
    ID STRING(MAX) NOT NULL,
    Arr1 ARRAY<STRING(MAX)>,
    Mark STRING(MAX),
    Col1 STRING(MAX),
    Col2 STRING(MAX),
    Col3 STRING(MAX),
    Col4 STRING(MAX),
    Col5 STRING(MAX),
    Col6 STRING(MAX),
    Col7 STRING(MAX),
    Col8 STRING(MAX),
    Col9 STRING(MAX),
    WithIndex1 STRING(MAX),
    WithIndex2 STRING(MAX),
    Storing1 STRING(MAX),
    Storing2 STRING(MAX),
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);

CREATE INDEX MeasureWithStoringWithIndex1_1
ON MeasureWithStoring (
    WithIndex1
)
STORING (Storing1);

CREATE INDEX MeasureWithStoringWithIndex2_1
ON MeasureWithStoring (
    WithIndex2
)
STORING (Storing1, Storing2);
