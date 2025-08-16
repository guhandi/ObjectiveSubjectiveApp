# ObjectiveSubjectiveHealth

## Overview
This project is a participant-first platform for collecting, managing, and licensing physiological data. It includes a web application built with FastAPI and a SQLite database to handle data acquisition from various tasks and surveys.

## Project Structure
- **src/**: Contains the main FastAPI application code.
- **tasks/**: Includes HTML files for surveys and cognition tasks.
  - **surveys/**: Contains survey HTML files like `daily_core.html`, `behavioral.html`, and `wellbeing.html`.
  - **cognition/**: Contains cognition task HTML files like `stroop_1min_v1.html`, `pvt.html`, and `wm.html`.
- **data/**: Directory for storing raw and processed data.
- **scripts/**: Python scripts for data analysis and feature extraction.
- **tests/**: Test cases and utilities.
- **configs/**: Configuration files.
- **docs/**: Documentation and additional resources.

## Setup
1. **Create a Conda Environment**:
   ```bash
   conda create --name HealthApp python=3.8
   conda activate HealthApp
   ```

2. **Install Dependencies**:
   ```bash
   conda install pandas numpy matplotlib seaborn scipy
   pip install fastapi uvicorn pydantic sqlite3
   ```

3. **Run the Application**:
   ```bash
   uvicorn src.main:app --reload
   ```

## Surveys and Tasks
- **Surveys**:
  - `daily_core.html`: Captures daily subjective feelings using Likert-scale sliders.
  - `behavioral.html`: Captures daily behaviors and exposures.
  - `wellbeing.html`: Captures weekly well-being and context.

- **Cognition Tasks**:
  - `stroop_1min_v1.html`: Stroop task to measure cognitive flexibility.
  - `pvt.html`: Psychomotor Vigilance Task to measure sustained attention.
  - `wm.html`: 1-Minute 1-Back Task to measure working memory.

## Next Steps
1. **Data Processing and Analysis**:
   - Develop Python scripts to process and analyze the collected data.
   - Implement feature extraction techniques.

2. **Visualization and Reporting**:
   - Create visualizations to represent the data and analysis results.
   - Generate reports or dashboards.

3. **Documentation**:
   - Update documentation to reflect the current state and usage of the application.

4. **Testing and Deployment**:
   - Conduct thorough testing and consider deploying the application to a cloud platform.

## Contributing
Contributions are welcome! Please feel free to submit a pull request or open an issue.

## License
This project is licensed under the MIT License. 