CREATE TABLE Measure (
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
    WithIndex1 STRING(MAX),
    WithIndex2 STRING(MAX),
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);

CREATE INDEX MeasureWithIndex1_1
ON Measure (
    WithIndex1
);

CREATE INDEX MeasureWithIndex2_1
ON Measure (
    WithIndex2
);

CREATE INDEX MeasureWithIndex2_2
ON Measure (
    WithIndex2 DESC
);
