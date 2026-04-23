CREATE TABLE IF NOT EXISTS polymarket_pred_vault_mapping (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pred_market_id VARCHAR(255) NOT NULL UNIQUE,
    pred_parent_market_id VARCHAR(255) NOT NULL,
    pred_fixture_id CHAR(36) NOT NULL,
    pred_league_id CHAR(36) NOT NULL,
    pred_home_team_id CHAR(36) NOT NULL,
    pred_away_team_id CHAR(36) NOT NULL,
    polymarket_market_id VARCHAR(255) NOT NULL,
    yes_token_id VARCHAR(255) NOT NULL,
    no_token_id VARCHAR(255) NOT NULL,
    home_team_id VARCHAR(255) NOT NULL,
    home_team_name VARCHAR(255) NOT NULL,
    away_team_id VARCHAR(255) NOT NULL,
    away_team_name VARCHAR(255) NOT NULL,
    league_id VARCHAR(255) NOT NULL,
    league_name VARCHAR(255) NOT NULL,
    game_id VARCHAR(255) NOT NULL,
    outcome_label VARCHAR(255) NOT NULL,
    match_score INT NOT NULL,
    match_reason TEXT NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'polymarket',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS polymarket_pred_vault_review_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pred_market_id VARCHAR(255) NOT NULL,
    pred_parent_market_id VARCHAR(255) NOT NULL,
    pred_fixture_id CHAR(36) NOT NULL,
    reason TEXT NOT NULL,
    top_score INT NOT NULL,
    candidate_market_ids JSON NOT NULL,
    candidate_reasons JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
