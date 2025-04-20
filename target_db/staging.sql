\c staging;

create extension if not exists "uuid-ossp";


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;



ALTER SCHEMA public OWNER TO postgres;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS '';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: acquisition; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.acquisition (
    acquisition_id integer NOT NULL,
    acquiring_object_id character varying(255),
    acquired_object_id character varying(255),
    term_code character varying(255),
    price_amount numeric(15,2),
    price_currency_code character varying(3),
    acquired_at timestamp without time zone,
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_acquisition UNIQUE (acquisition_id)
);

ALTER TABLE public.acquisition OWNER TO postgres;

--
-- Name: company; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.company (
    office_id integer NOT NULL,
    object_id character varying(255),
    description text,
    region character varying(255),
    address1 text,
    address2 text,
    city character varying(255),
    zip_code character varying(200),
    state_code character varying(255),
    country_code character varying(255),
    latitude numeric(9,6),
    longitude numeric(9,6),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_company UNIQUE (office_id)
);

ALTER TABLE public.company OWNER TO postgres;

--
-- Name: funding_rounds; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.funding_rounds (
    funding_round_id integer NOT NULL,
    object_id character varying(255),
    funded_at date,
    funding_round_type character varying(255),
    funding_round_code character varying(255),
    raised_amount_usd numeric(15,2),
    raised_amount numeric(15,2),
    raised_currency_code character varying(255),
    pre_money_valuation_usd numeric(15,2),
    pre_money_valuation numeric(15,2),
    pre_money_currency_code character varying(255),
    post_money_valuation_usd numeric(15,2),
    post_money_valuation numeric(15,2),
    post_money_currency_code character varying(255),
    participants text,
    is_first_round boolean,
    is_last_round boolean,
    source_url text,
    source_description text,
    created_by character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_funding_rounds UNIQUE (funding_round_id)
);

ALTER TABLE public.funding_rounds OWNER TO postgres;

--
-- Name: funds; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.funds (
    fund_id character varying(255) NOT NULL,
    object_id character varying(255),
    name character varying(255),
    funded_at date,
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_funds UNIQUE (fund_id)
);

ALTER TABLE public.funds OWNER TO postgres;

--
-- Name: investments; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.investments (
    investment_id integer NOT NULL,
    funding_round_id integer,
    funded_object_id character varying,
    investor_object_id character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_investments UNIQUE (investment_id)
);

ALTER TABLE public.investments OWNER TO postgres;

--
-- Name: ipos; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ipos (
    ipo_id character varying(255) NOT NULL,
    object_id character varying(255),
    valuation_amount numeric(15,2),
    valuation_currency_code character varying(3),
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    public_at timestamp without time zone,
    stock_symbol character varying(255),
    source_url text,
    source_description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_ipos UNIQUE (ipo_id)
);

ALTER TABLE public.ipos OWNER TO postgres;


CREATE TABLE public.people(
    people_id character varying(255) NOT NULL,
    object_id character varying(255) NOT NULL,
    first_name character varying(255),
    last_name character varying(255),
    birthplace character varying(255),
    affiliation_name character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_people UNIQUE (people_id)
);

ALTER TABLE public.people OWNER TO postgres;

CREATE TABLE public.relationships(
    relationship_id character varying(255) NOT NULL,
    person_object_id character varying(255) NOT NULL,
    relationship_object_id character varying(255),
    start_at date,
    end_at date,
    is_past boolean,
    sequence integer,
    title text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_relationships UNIQUE (relationship_id)
);

ALTER TABLE public.people OWNER TO postgres;


