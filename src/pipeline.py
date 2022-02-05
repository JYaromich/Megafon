import pandas as pd
import numpy as np

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer


class ColumnSelector(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        assert isinstance(X, pd.DataFrame)

        try:
            return X.loc[:, self.columns]
        except KeyError:
            cols_error = list(set(self.columns) - set(X.columns))
            raise KeyError("DataFrame не содердит следующие колонки: %s" % cols_error)
        

class DropColumns(BaseEstimator, TransformerMixin):
    def __init__(self, columns) -> None:
        self.columns = columns
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
#         if not isinstance(X, pd.DataFrame):
#             raise TypeError('Input data must be pd.DataFrame')
        
        try:
            return X.drop(columns=self.columns, axis=1)
            
        except KeyError:
            cols_error = list(set(self.columns) - set(X.columns))
            raise KeyError("DataFrame doesn't contain this coluumn name: %s" % cols_error)


class HandleOutliners(BaseEstimator, TransformerMixin):
    def __init__(self, quantile=.99) -> None:
        self.quantile = quantile
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X, y=None):
        for col in X.columns.to_list():
            X.loc[X[col] > X[col].quantile(q=self.quantile), col] = np.nan
        return X


class CustumSimpleImputer(SimpleImputer):
    def transform(self, X):
        column_list = X.columns.to_list()        
        return pd.DataFrame(data=super().transform(X), columns=column_list)
    
    
class SetCategoryType(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.type_name = 'category'
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X, y=None):
        return X.astype(self.type_name).categorize(columns=list(CATEGORIES_FEATURES))
    
    
class ChangeDtypes(BaseEstimator, TransformerMixin):
    def __init__(self, dtype_name, columns_list) -> None:
        self.dtype_name = dtype_name
        self.columns_list = columns_list
        
    def fit(self, X, y=None):
        return self
    
    def transform(self, X, y=None):
        X[self.columns_list] = X[self.columns_list].astype(self.dtype_name)
        return X