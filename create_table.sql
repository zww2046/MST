DROP TABLE public.equity_market;

CREATE TABLE public.equity_market
(
    volume integer,
    symbol character varying COLLATE pg_catalog."default",
    open double precision,
    low double precision,
    high double precision,
    date date,
    close double precision,
    adj_close double precision
)

DROP TABLE public.symbol;

CREATE TABLE public.symbol
(
    symbol character varying COLLATE pg_catalog."default",
    remarks character varying COLLATE pg_catalog."default",
    exchange character varying COLLATE pg_catalog."default",
    market_category character varying COLLATE pg_catalog."default",
    etf_flag boolean,
    lot_size integer,
    financial_status character varying COLLATE pg_catalog."default",
    cqs_symbol character varying COLLATE pg_catalog."default",
    nextshares boolean,
    creation_date date
)

DROP TABLE public.temp_equity_market;

CREATE TABLE public.temp_equity_market
(
    volume integer,
    symbol character varying COLLATE pg_catalog."default",
    open double precision,
    low double precision,
    high double precision,
    date date,
    close double precision,
    adj_close double precision
)

DROP TABLE public.temp_symbol;

CREATE TABLE public.temp_symbol
(
    symbol character varying COLLATE pg_catalog."default",
    remarks character varying COLLATE pg_catalog."default",
    exchange character varying COLLATE pg_catalog."default",
    market_category character varying COLLATE pg_catalog."default",
    etf_flag boolean,
    lot_size integer,
    financial_status character varying COLLATE pg_catalog."default",
    cqs_symbol character varying COLLATE pg_catalog."default",
    nextshares boolean
)