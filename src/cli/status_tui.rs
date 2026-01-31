use std::io;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};
use ratatui::{Frame, Terminal};

use crate::cli::{Client, ClientError};
use crate::protocol::ListResult;

#[derive(Debug, Clone)]
struct RepoRow {
    name: String,
    generations: usize,
    commits: usize,
    size_bytes: u64,
    last_sync: String,
}

#[derive(Debug, Clone, Default)]
struct ViewState {
    rows: Vec<RepoRow>,
    error: Option<String>,
}

pub fn run_status_tui() -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = ViewState::default();
    let mut last_poll = Instant::now() - Duration::from_secs(2);

    loop {
        if last_poll.elapsed() >= Duration::from_secs(1) {
            state = refresh_state();
            last_poll = Instant::now();
        }

        terminal.draw(|frame| render(frame, &state))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn refresh_state() -> ViewState {
    let mut client = match Client::connect() {
        Ok(client) => client,
        Err(ClientError::NotRunning) => {
            return ViewState {
                error: Some("daemon is not running".to_string()),
                ..ViewState::default()
            };
        }
        Err(err) => {
            return ViewState {
                error: Some(format!("failed to connect: {}", err)),
                ..ViewState::default()
            };
        }
    };

    let list = match client.list() {
        Ok(list) => list,
        Err(err) => {
            return ViewState {
                error: Some(format!("failed to fetch list: {}", err)),
                ..ViewState::default()
            };
        }
    };

    let rows = build_rows(list);
    ViewState { rows, error: None }
}

fn build_rows(list: ListResult) -> Vec<RepoRow> {
    let mut rows = Vec::with_capacity(list.repos.len());
    for repo in list.repos {
        let name = format!("{}/{}", repo.owner, repo.repo);
        let last_sync = repo.last_sync.unwrap_or_else(|| "unknown".to_string());
        rows.push(RepoRow {
            name,
            generations: repo.generation_count as usize,
            commits: repo.commit_count as usize,
            size_bytes: repo.total_size_bytes,
            last_sync,
        });
    }

    rows
}

fn render(frame: &mut Frame, state: &ViewState) {
    let size = frame.area();
    let chunks = Layout::default()
        .constraints([Constraint::Percentage(100)])
        .split(size);

    if let Some(error) = &state.error {
        let block = Block::default().title("ghfs status").borders(Borders::ALL);
        let row = Row::new(vec![Cell::from(Line::from(error.as_str()))]);
        let table = Table::new(vec![row], [Constraint::Percentage(100)]).block(block);
        frame.render_widget(table, chunks[0]);
        return;
    }

    let header = Row::new(vec![
        Cell::from("REPO"),
        Cell::from("GENS"),
        Cell::from("COMMITS"),
        Cell::from("SIZE"),
        Cell::from("LAST SYNC"),
    ])
    .style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let rows = state.rows.iter().map(|row| {
        Row::new(vec![
            Cell::from(row.name.clone()),
            Cell::from(row.generations.to_string()),
            Cell::from(row.commits.to_string()),
            Cell::from(format_bytes(row.size_bytes)),
            Cell::from(row.last_sync.clone()),
        ])
    });

    let block = Block::default().title("ghfs status").borders(Borders::ALL);
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(45),
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(12),
        ],
    )
    .header(header)
    .block(block)
    .column_spacing(1);

    frame.render_widget(table, chunks[0]);
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}
