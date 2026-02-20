CREATE DATABASE IF NOT EXISTS funds_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE funds_db;

CREATE TABLE IF NOT EXISTS stg_ft_master_ticker (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ft_ticker VARCHAR(64) NOT NULL,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ft_master_ft_ticker (ft_ticker),
  KEY idx_ft_master_ticker (ticker),
  KEY idx_ft_master_type (ticker_type),
  KEY idx_ft_master_date_scraper (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_ft_daily_nav (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ft_ticker VARCHAR(64) NOT NULL,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  nav_price DECIMAL(20,8) NULL,
  nav_currency VARCHAR(16) NULL,
  nav_as_of DATE NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ft_nav_ft_ticker_asof (ft_ticker, nav_as_of),
  KEY idx_ft_nav_ticker (ticker),
  KEY idx_ft_nav_date_scraper (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_ft_static_detail (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ft_ticker VARCHAR(64) NOT NULL,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  morningstar_category VARCHAR(255) NULL,
  inception_date DATE NULL,
  domicile VARCHAR(128) NULL,
  isin_number VARCHAR(32) NULL,
  assets_aum_raw VARCHAR(255) NULL,
  assets_aum_full_value DECIMAL(22,2) NULL,
  assets_aum_value DECIMAL(22,8) NULL,
  assets_aum_unit VARCHAR(16) NULL,
  assets_aum_currency VARCHAR(16) NULL,
  assets_aum_as_of DATE NULL,
  expense_ratio_raw VARCHAR(64) NULL,
  expense_pct DECIMAL(10,4) NULL,
  income_treatment VARCHAR(128) NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ft_static_ft_ticker_date (ft_ticker, date_scraper),
  KEY idx_ft_static_ticker (ticker),
  KEY idx_ft_static_date_scraper (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_ft_holdings (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  allocation_type VARCHAR(64) NOT NULL,
  holding_name VARCHAR(512) NOT NULL,
  holding_ticker VARCHAR(64) NULL,
  holding_type VARCHAR(32) NULL,
  holding_symbol VARCHAR(32) NULL,
  holding_url VARCHAR(1024) NULL,
  portfolio_weight_pct DECIMAL(10,4) NULL,
  top_10_holdings_weight_pct DECIMAL(10,4) NULL,
  other_holding_weight_pct DECIMAL(10,4) NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_ft_holdings_ticker (ticker),
  KEY idx_ft_holdings_date_scraper (date_scraper),
  KEY idx_ft_holdings_allocation_type (allocation_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_ft_sector_region (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ft_ticker VARCHAR(64) NOT NULL,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  category_name VARCHAR(255) NOT NULL,
  weight_pct DECIMAL(10,4) NULL,
  allocation_type VARCHAR(64) NOT NULL,
  url_type_used VARCHAR(64) NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Financial Times',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_ft_sr_ft_ticker (ft_ticker),
  KEY idx_ft_sr_date_scraper (date_scraper),
  KEY idx_ft_sr_allocation_type (allocation_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_master_ticker (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_master_ticker (ticker),
  KEY idx_yf_master_type (ticker_type),
  KEY idx_yf_master_date_scraper (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_master_ticker (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NOT NULL,
  ticker_type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Stock Analysis',
  date_scraper DATE NOT NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_master_ticker (ticker),
  KEY idx_sa_master_type (ticker_type),
  KEY idx_sa_master_date_scraper (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_daily_nav (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
  nav_price DECIMAL(20,8) NULL,
  currency VARCHAR(16) NULL,
  as_of_date DATE NULL,
  scrape_date DATE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_nav_ticker_asof (ticker, as_of_date),
  KEY idx_yf_nav_asset_type (asset_type),
  KEY idx_yf_nav_scrape_date (scrape_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_daily_nav (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Stock Analysis',
  nav_price DECIMAL(20,8) NULL,
  currency VARCHAR(16) NULL,
  as_of_date DATE NULL,
  scrape_date DATE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_nav_ticker_asof (ticker, as_of_date),
  KEY idx_sa_nav_asset_type (asset_type),
  KEY idx_sa_nav_scrape_date (scrape_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS daily_fx_rates (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  rate_date DATE NOT NULL,
  from_currency VARCHAR(16) NOT NULL,
  to_currency VARCHAR(16) NOT NULL,
  fx_rate DECIMAL(20,10) NOT NULL,
  provider VARCHAR(64) NOT NULL DEFAULT 'frankfurter',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_fx_rate_date_pair (rate_date, from_currency, to_currency),
  KEY idx_fx_from_to_date (from_currency, to_currency, rate_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_static_identity (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(512) NULL,
  exchange VARCHAR(128) NULL,
  issuer VARCHAR(255) NULL,
  category VARCHAR(255) NULL,
  inception_date VARCHAR(64) NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'Yahoo Finance',
  updated_at DATE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_identity_ticker_date (ticker, updated_at),
  KEY idx_yf_identity_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_static_fees (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  expense_ratio VARCHAR(64) NULL,
  initial_charge VARCHAR(64) NULL,
  exit_charge VARCHAR(64) NULL,
  assets_aum VARCHAR(128) NULL,
  top_10_hold_pct VARCHAR(64) NULL,
  holdings_count VARCHAR(64) NULL,
  holdings_turnover VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_fees_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_static_risk (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  morningstar_rating VARCHAR(16) NULL,
  alpha_3y VARCHAR(64) NULL,
  alpha_5y VARCHAR(64) NULL,
  alpha_10y VARCHAR(64) NULL,
  beta_3y VARCHAR(64) NULL,
  beta_5y VARCHAR(64) NULL,
  beta_10y VARCHAR(64) NULL,
  mean_annual_return_3y VARCHAR(64) NULL,
  mean_annual_return_5y VARCHAR(64) NULL,
  mean_annual_return_10y VARCHAR(64) NULL,
  r_squared_3y VARCHAR(64) NULL,
  r_squared_5y VARCHAR(64) NULL,
  r_squared_10y VARCHAR(64) NULL,
  standard_deviation_3y VARCHAR(64) NULL,
  standard_deviation_5y VARCHAR(64) NULL,
  standard_deviation_10y VARCHAR(64) NULL,
  sharpe_ratio_3y VARCHAR(64) NULL,
  sharpe_ratio_5y VARCHAR(64) NULL,
  sharpe_ratio_10y VARCHAR(64) NULL,
  treynor_ratio_3y VARCHAR(64) NULL,
  treynor_ratio_5y VARCHAR(64) NULL,
  treynor_ratio_10y VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_risk_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_static_policy (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  div_yield VARCHAR(64) NULL,
  pe_ratio VARCHAR(64) NULL,
  total_return_ytd VARCHAR(64) NULL,
  total_return_1y VARCHAR(64) NULL,
  updated_at DATE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_yf_policy_ticker_date (ticker, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_holdings (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  yahoo_ticker VARCHAR(32) NULL,
  asset_type VARCHAR(32) NULL,
  symbol VARCHAR(64) NULL,
  name VARCHAR(512) NULL,
  value VARCHAR(128) NULL,
  updated_at DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_yf_holdings_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_sectors (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  sector VARCHAR(255) NOT NULL,
  value VARCHAR(128) NULL,
  updated_at DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_yf_sectors_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_yf_allocation (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  category VARCHAR(255) NOT NULL,
  value VARCHAR(128) NULL,
  updated_at DATE NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_yf_alloc_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_static_info (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  source VARCHAR(64) NULL,
  name VARCHAR(512) NULL,
  isin_number VARCHAR(32) NULL,
  cusip_number VARCHAR(32) NULL,
  issuer VARCHAR(255) NULL,
  category VARCHAR(255) NULL,
  index_benchmark VARCHAR(255) NULL,
  inception_date VARCHAR(64) NULL,
  exchange VARCHAR(64) NULL,
  region VARCHAR(128) NULL,
  country VARCHAR(128) NULL,
  leverage VARCHAR(64) NULL,
  options VARCHAR(64) NULL,
  shares_out VARCHAR(64) NULL,
  market_cap_size VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_info_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_static_fees (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  source VARCHAR(64) NULL,
  expense_ratio VARCHAR(64) NULL,
  initial_charge VARCHAR(64) NULL,
  exit_charge VARCHAR(64) NULL,
  assets_aum VARCHAR(128) NULL,
  top_10_hold_pct VARCHAR(64) NULL,
  holdings_count VARCHAR(64) NULL,
  holdings_turnover VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_fees_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_static_risk (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  source VARCHAR(64) NULL,
  sharpe_ratio_5y VARCHAR(64) NULL,
  beta_5y VARCHAR(64) NULL,
  rsi_daily VARCHAR(64) NULL,
  moving_avg_200 VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_risk_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_static_policy (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  asset_type VARCHAR(32) NULL,
  source VARCHAR(64) NULL,
  div_yield VARCHAR(64) NULL,
  div_growth_1y VARCHAR(64) NULL,
  div_growth_3y VARCHAR(64) NULL,
  div_growth_5y VARCHAR(64) NULL,
  div_growth_10y VARCHAR(64) NULL,
  div_consecutive_years VARCHAR(64) NULL,
  payout_ratio VARCHAR(64) NULL,
  total_return_ytd VARCHAR(64) NULL,
  total_return_1y VARCHAR(64) NULL,
  pe_ratio VARCHAR(64) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sa_policy_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_holdings (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  file_name VARCHAR(255) NOT NULL,
  downloaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_sa_holdings_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stg_sa_sector_country (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ticker VARCHAR(32) NOT NULL,
  category_name VARCHAR(255) NOT NULL,
  percentage DECIMAL(10,4) NULL,
  type VARCHAR(32) NOT NULL,
  source VARCHAR(64) NULL,
  date_scraper DATE NULL,
  url VARCHAR(1024) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_sa_sector_country_ticker (ticker),
  KEY idx_sa_sector_country_type (type),
  KEY idx_sa_sector_country_date (date_scraper)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
