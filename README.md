# ObjectiveSubjectiveHealth

## Overview
ObjectiveSubjectiveHealth is a participant-first platform designed to revolutionize the collection, management, and licensing of physiological data. The system is built to support a wide range of data types, including EEG, wearables, and cognitive task data, with a focus on participant engagement and data quality. The platform consists of three main components:

1. **Participant App (Front-End)**: A mobile and web application where participants can upload data from supported devices, complete structured tasks, and view their data quality and usage history.

2. **Data Infrastructure (Back-End Engine)**: A cloud-native system for real-time data ingestion, validation, and processing, with AI-powered preprocessing pipelines and a smart royalty engine for data monetization.

3. **Research & Licensing Dashboard (Partner Portal)**: A secure interface for researchers and developers to access and license structured datasets for various applications.

## Project Structure
- **src/**: Contains the main FastAPI application code, including API endpoints and database interactions.
- **tasks/**: Includes HTML files for surveys and cognition tasks.
  - **surveys/**: Contains survey HTML files like `daily_core.html`, `behavioral.html`, and `wellbeing.html`.
  - **cognition/**: Contains cognition task HTML files like `stroop.html`, `pvt.html`, and `wm.html`.
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
  - `stroop.html`: Stroop task to measure cognitive flexibility.
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