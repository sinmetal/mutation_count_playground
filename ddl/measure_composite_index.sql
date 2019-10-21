CREATE TABLE MeasureCompositeIndex (
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
    WithCompositeIndex1 STRING(MAX),
    WithCompositeIndex2 STRING(MAX),
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);

CREATE INDEX MeasureCompositeIndexWithIndex1_1
ON MeasureCompositeIndex (
    WithIndex1
);

CREATE INDEX MeasureCompositeIndexWithIndex2_1
ON MeasureCompositeIndex (
    WithIndex2
);

CREATE INDEX MeasureCompositeIndexWithIndex2_2
ON MeasureCompositeIndex (
    WithIndex2 DESC
);

CREATE INDEX MeasureCompositeIndexWithCompositeIndex
ON MeasureCompositeIndex (
    WithCompositeIndex1,
    WithCompositeIndex2 DESC
);
