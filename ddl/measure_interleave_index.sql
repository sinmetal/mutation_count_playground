CREATE TABLE MeasureParentWithIndex (
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
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);

CREATE TABLE MeasureChildWithIndex (
    ID STRING(MAX) NOT NULL,
    ChildID STRING(MAX) NOT NULL,
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
    CommitedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID, ChildID),
  INTERLEAVE IN PARENT MeasureParentWithIndex ON DELETE CASCADE;

CREATE INDEX MeasureChildWithIndexWithIndex1_1
ON MeasureChildWithIndex (
    WithIndex1
);