CREATE TABLE IF NOT EXISTS polymarket_pred_vault_mapping (
    id BIGSERIAL PRIMARY KEY,
    pred_market_id TEXT NOT NULL UNIQUE,
    pred_parent_market_id TEXT NOT NULL,
    pred_fixture_id UUID NOT NULL,
    pred_league_id UUID NOT NULL,
    pred_home_team_id UUID NOT NULL,
    pred_away_team_id UUID NOT NULL,
    polymarket_market_id TEXT NOT NULL,
    yes_token_id TEXT NOT NULL,
    no_token_id TEXT NOT NULL,
    home_team_id TEXT NOT NULL,
    home_team_name TEXT NOT NULL,
    away_team_id TEXT NOT NULL,
    away_team_name TEXT NOT NULL,
    league_id TEXT NOT NULL,
    league_name TEXT NOT NULL,
    game_id TEXT NOT NULL,
    outcome_label TEXT NOT NULL,
    match_score INTEGER NOT NULL,
    match_reason TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'polymarket',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS polymarket_pred_vault_review_queue (
    id BIGSERIAL PRIMARY KEY,
    pred_market_id TEXT NOT NULL,
    pred_parent_market_id TEXT NOT NULL,
    pred_fixture_id UUID NOT NULL,
    reason TEXT NOT NULL,
    top_score INTEGER NOT NULL,
    candidate_market_ids JSONB NOT NULL,
    candidate_reasons JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
