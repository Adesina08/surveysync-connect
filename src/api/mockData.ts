import type { SurveyCTOForm, PostgresSchema } from './types';

export const mockForms: SurveyCTOForm[] = [
  {
    id: 'household_survey_2024',
    name: 'Household Survey 2024',
    version: '3.2',
    responses: 1247,
    lastUpdated: '2024-01-15T14:30:00Z',
    fields: [
      { name: 'KEY', type: 'text', label: 'Unique ID', isPrimaryKey: true },
      { name: 'SubmissionDate', type: 'datetime', label: 'Submission Date', isPrimaryKey: false },
      { name: 'CompletionDate', type: 'datetime', label: 'Completion Date', isPrimaryKey: false },
      { name: 'household_id', type: 'text', label: 'Household ID', isPrimaryKey: false },
      { name: 'respondent_name', type: 'text', label: 'Respondent Name', isPrimaryKey: false },
      { name: 'household_size', type: 'integer', label: 'Household Size', isPrimaryKey: false },
      { name: 'monthly_income', type: 'decimal', label: 'Monthly Income', isPrimaryKey: false },
      { name: 'district', type: 'select_one', label: 'District', isPrimaryKey: false },
      { name: 'gps_location', type: 'geopoint', label: 'GPS Location', isPrimaryKey: false },
    ],
  },
  {
    id: 'health_assessment_q1',
    name: 'Health Assessment Q1',
    version: '2.1',
    responses: 892,
    lastUpdated: '2024-01-12T09:15:00Z',
    fields: [
      { name: 'KEY', type: 'text', label: 'Unique ID', isPrimaryKey: true },
      { name: 'SubmissionDate', type: 'datetime', label: 'Submission Date', isPrimaryKey: false },
      { name: 'CompletionDate', type: 'datetime', label: 'Completion Date', isPrimaryKey: false },
      { name: 'patient_id', type: 'text', label: 'Patient ID', isPrimaryKey: false },
      { name: 'age', type: 'integer', label: 'Age', isPrimaryKey: false },
      { name: 'gender', type: 'select_one', label: 'Gender', isPrimaryKey: false },
      { name: 'symptoms', type: 'select_multiple', label: 'Symptoms', isPrimaryKey: false },
      { name: 'diagnosis_date', type: 'date', label: 'Diagnosis Date', isPrimaryKey: false },
    ],
  },
  {
    id: 'education_tracking',
    name: 'Education Tracking Form',
    version: '1.5',
    responses: 2341,
    lastUpdated: '2024-01-10T16:45:00Z',
    fields: [
      { name: 'KEY', type: 'text', label: 'Unique ID', isPrimaryKey: true },
      { name: 'SubmissionDate', type: 'datetime', label: 'Submission Date', isPrimaryKey: false },
      { name: 'CompletionDate', type: 'datetime', label: 'Completion Date', isPrimaryKey: false },
      { name: 'student_id', type: 'text', label: 'Student ID', isPrimaryKey: false },
      { name: 'school_name', type: 'text', label: 'School Name', isPrimaryKey: false },
      { name: 'grade_level', type: 'integer', label: 'Grade Level', isPrimaryKey: false },
      { name: 'attendance_rate', type: 'decimal', label: 'Attendance Rate', isPrimaryKey: false },
      { name: 'test_score', type: 'decimal', label: 'Test Score', isPrimaryKey: false },
    ],
  },
  {
    id: 'water_sanitation',
    name: 'Water & Sanitation Survey',
    version: '4.0',
    responses: 567,
    lastUpdated: '2024-01-08T11:20:00Z',
    fields: [
      { name: 'KEY', type: 'text', label: 'Unique ID', isPrimaryKey: true },
      { name: 'SubmissionDate', type: 'datetime', label: 'Submission Date', isPrimaryKey: false },
      { name: 'CompletionDate', type: 'datetime', label: 'Completion Date', isPrimaryKey: false },
      { name: 'village_id', type: 'text', label: 'Village ID', isPrimaryKey: false },
      { name: 'water_source', type: 'select_one', label: 'Water Source', isPrimaryKey: false },
      { name: 'distance_to_water', type: 'decimal', label: 'Distance to Water (km)', isPrimaryKey: false },
      { name: 'has_latrine', type: 'select_one', label: 'Has Latrine', isPrimaryKey: false },
      { name: 'gps_coordinates', type: 'geopoint', label: 'GPS Coordinates', isPrimaryKey: false },
    ],
  },
];

export const mockSchemas: PostgresSchema[] = [
  {
    name: 'public',
    tables: [
      {
        name: 'household_data',
        primaryKey: 'id',
        rowCount: 1200,
        columns: [
          { name: 'id', type: 'text', nullable: false, isPrimaryKey: true },
          { name: 'created_at', type: 'timestamptz', nullable: false, isPrimaryKey: false },
          { name: 'household_id', type: 'text', nullable: true, isPrimaryKey: false },
          { name: 'respondent_name', type: 'text', nullable: true, isPrimaryKey: false },
          { name: 'household_size', type: 'integer', nullable: true, isPrimaryKey: false },
        ],
      },
      {
        name: 'health_records',
        primaryKey: 'record_id',
        rowCount: 850,
        columns: [
          { name: 'record_id', type: 'text', nullable: false, isPrimaryKey: true },
          { name: 'patient_id', type: 'text', nullable: true, isPrimaryKey: false },
          { name: 'assessment_date', type: 'date', nullable: true, isPrimaryKey: false },
          { name: 'diagnosis', type: 'text', nullable: true, isPrimaryKey: false },
        ],
      },
    ],
  },
  {
    name: 'surveys',
    tables: [
      {
        name: 'raw_responses',
        primaryKey: 'key',
        rowCount: 3500,
        columns: [
          { name: 'key', type: 'text', nullable: false, isPrimaryKey: true },
          { name: 'form_id', type: 'text', nullable: false, isPrimaryKey: false },
          { name: 'submitted_at', type: 'timestamptz', nullable: false, isPrimaryKey: false },
          { name: 'data', type: 'jsonb', nullable: true, isPrimaryKey: false },
        ],
      },
    ],
  },
  {
    name: 'staging',
    tables: [],
  },
];

// Simulate network delay
export const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Simulate random failures for testing error handling
export const shouldFail = (probability: number = 0.1) => Math.random() < probability;
