from pathlib import Path

TARGET_COLUMN_NAME = 'target'
RANDOM_STATE = 42
DATA_ROOT = Path('E:/2. УЧЕБА ЭНИ')
SAVED_DATA_ROOT = Path('C:/Users/Евгений/Desktop/')
FEATURES_PATH = DATA_ROOT / 'features.csv'
TRAIN_PATH = DATA_ROOT / 'data_train.csv'
TEST_PATH = DATA_ROOT / 'data_test.csv'
MERGE_TEST_PATH = SAVED_DATA_ROOT / 'test_merge.csv'
MERGE_TRAIN_PATH = SAVED_DATA_ROOT / 'train_merge.csv'

ROOT_TO_SAVE_MODEL = Path('./')
PATH_TO_SAVE_MODEL = ROOT_TO_SAVE_MODEL / 'model.pkl'

PATH_TO_SAVE_PREDICTION_ROOT = Path('./')
PATH_TO_SAVE_PREDICTION = PATH_TO_SAVE_PREDICTION_ROOT / 'prediction.csv'