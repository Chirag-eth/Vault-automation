-- Polymarket leagues reference export
CREATE TABLE IF NOT EXISTS polymarket_leagues_reference (
    league_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    alternate_name TEXT NOT NULL,
    sport TEXT NOT NULL,
    association TEXT NOT NULL
);

DELETE FROM polymarket_leagues_reference;
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (2, 'Premier League', 'epl', 'soccer', 'premierleague.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (3, 'LaLiga', 'lal', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (95, 'ACN', 'acn', 'soccer', 'cafonline.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (7, 'Bundesliga', 'bun', 'soccer', 'bundesliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (11, 'Ligue 1', 'fl1', 'soccer', 'ligue1.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (12, 'Serie A', 'sea', 'soccer', 'legaseriea.it');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (13, 'UEFA Champions League', 'ucl', 'soccer', 'uefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (15, 'AFC', 'afc', 'soccer', 'the-afc.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (16, 'OFC', 'ofc', 'soccer', 'oceaniafootball.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (17, 'FIF', 'fif', 'soccer', 'fifa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (18, 'ERE', 'ere', 'soccer', 'eredivisie.eu');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (19, 'ARG', 'arg', 'soccer', 'afa.com.ar');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (20, 'ITC', 'itc', 'soccer', 'legaseriea.it');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (21, 'MEX', 'mex', 'soccer', 'ligamx.net');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (23, 'LIB', 'lib', 'soccer', 'conmebollibertadores.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (24, 'SUD', 'sud', 'soccer', 'conmebolsudamericana.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (25, 'TUR', 'tur', 'soccer', 'tff.org');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (26, 'CON', 'con', 'soccer', 'conmebol.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (27, 'COF', 'cof', '', 'concacaf.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (28, 'UEF', 'uef', 'soccer', 'uefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (29, 'CAF', 'caf', 'soccer', 'cafonline.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (30, 'RUS', 'rus', '', 'premierliga.ru');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (31, 'EFA', 'efa', 'soccer', 'thefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (32, 'EFL', 'efl', 'soccer', 'efl.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (36, 'UEL', 'uel', 'soccer', 'uefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (33, 'MLS', 'mls', '', 'mls.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (49, 'CDR', 'cdr', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (61, 'COL', 'col', 'soccer', 'uefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (65, 'JAP', 'jap', 'soccer', 'jleague.jp');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (66, 'JA2', 'ja2', 'soccer', 'jleague.jp');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (67, 'KOR', 'kor', 'soccer', 'kleague.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (74, 'POR', 'por', 'soccer', 'ligaportugal.pt');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (99, 'SSC', 'ssc', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (120, 'MAR1', 'mar1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (121, 'EGY1', 'egy1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (122, 'CZE1', 'cze1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (123, 'BOL1', 'bol1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (124, 'ROU1', 'rou1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (125, 'BRA2', 'bra2', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (126, 'PER1', 'per1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (149, 'UWCL', 'uwcl', 'soccer', 'uefa.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (157, 'J2-100', 'j2-100', 'soccer', 'jleague.jp');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (127, 'COL1', 'col1', 'soccer', 'laliga.com');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (158, 'J1-100', 'j1-100', 'soccer', 'jleague.jp');
INSERT INTO polymarket_leagues_reference (league_id, name, alternate_name, sport, association) VALUES (128, 'CHI1', 'chi1', 'soccer', 'laliga.com');
