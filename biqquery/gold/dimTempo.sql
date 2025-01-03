CREATE OR REPLACE PROCEDURE `dwbq-445617`.dw_gold.dim_Tempo(start_date DATE, end_date DATE)
BEGIN
  
  truncate table `dwbq-445617`.dw_gold.DIM_TEMPO;

  -- Insere os registros na tabela DW.GOLD.DIM_TEMPO
  INSERT INTO `dwbq-445617`.dw_gold.DIM_TEMPO (
    SK_DATA,
    DATA,
    ANO,
    MES,
    DIA,
    DIA_SEMANA,
    DIA_ANO,
    ANO_BISSEXTO,
    DIA_UTIL,
    FIM_SEMANA,
    FERIADO,
    PRE_FERIADO,
    POS_FERIADO,
    NOME_FERIADO,
    NOME_DIA_SEMANA,
    NOME_DIA_SEMANA_ABREV,
    NOME_MES,
    NOME_MES_ABREV,
    QUINZENA,
    BIMESTRE,
    TRIMESTRE,
    SEMESTRE,
    NR_SEMANA_MES,
    NR_SEMANA_ANO,
    DATA_POR_EXTENSO,
    EVENTO
  )
SELECT 
    ROW_NUMBER() OVER () AS SK_DATA, -- Gera um SK sequencial
    d.DATA,
    EXTRACT(YEAR FROM d.DATA) AS ANO,
    EXTRACT(MONTH FROM d.DATA) AS MES,
    EXTRACT(DAY FROM d.DATA) AS DIA,
    EXTRACT(DAYOFWEEK FROM d.DATA) AS DIA_SEMANA,
    EXTRACT(DAYOFYEAR FROM d.DATA) AS DIA_ANO,
    CASE 
      WHEN MOD(EXTRACT(YEAR FROM d.DATA), 4) = 0 AND 
           (MOD(EXTRACT(YEAR FROM d.DATA), 100) != 0 OR MOD(EXTRACT(YEAR FROM d.DATA), 400) = 0) THEN 'S'
      ELSE 'N'
    END AS ANO_BISSEXTO,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM d.DATA) IN (1, 7) THEN 'N'
      ELSE 'S'
    END AS DIA_UTIL,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM d.DATA) IN (1, 7) THEN 'S'
      ELSE 'N'
    END AS FIM_SEMANA,
    'N' AS FERIADO, -- Substituir com lógica real de feriados, se disponível
    'N' AS PRE_FERIADO, -- Substituir com lógica real de pré-feriados
    'N' AS POS_FERIADO, -- Substituir com lógica real de pós-feriados
    'N' AS NOME_FERIADO, -- Substituir com lógica real de feriados
    FORMAT_TIMESTAMP('%A', TIMESTAMP(d.DATA)) AS NOME_DIA_SEMANA,
    FORMAT_TIMESTAMP('%a', TIMESTAMP(d.DATA)) AS NOME_DIA_SEMANA_ABREV,
    FORMAT_TIMESTAMP('%B', TIMESTAMP(d.DATA)) AS NOME_MES,
    FORMAT_TIMESTAMP('%b', TIMESTAMP(d.DATA)) AS NOME_MES_ABREV,
    CASE
      WHEN EXTRACT(DAY FROM d.DATA) <= 15 THEN 1
      ELSE 2
    END AS QUINZENA,
    CAST(CEIL(EXTRACT(MONTH FROM d.DATA) / 2) AS INT64) AS BIMESTRE,
    CAST(CEIL(EXTRACT(MONTH FROM d.DATA) / 3) AS INT64) AS TRIMESTRE,
    CAST(CEIL(EXTRACT(MONTH FROM d.DATA) / 6) AS INT64) AS SEMESTRE,
    CAST(CEIL(EXTRACT(DAY FROM d.DATA) / 7) AS INT64 )  AS NR_SEMANA_MES,
    CAST(EXTRACT(WEEK FROM d.DATA) AS INT64 ) AS NR_SEMANA_ANO,
    FORMAT_TIMESTAMP('%d de %B de %Y', TIMESTAMP(d.DATA)) AS DATA_POR_EXTENSO,
    'N' AS EVENTO -- Adicionar lógica para eventos, se aplicável
  FROM (
    SELECT 
      DATE_ADD(start_date, INTERVAL n DAY) AS DATA
    FROM 
      UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
  ) d;
END;


CALL `dwbq-445617`.dw_gold.dim_Tempo('2024-01-01', '2024-12-31')