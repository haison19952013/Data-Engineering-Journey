import unittest
import pandas as pd
import numpy as np
from data_pipeline import wrangle_salary, wrangle_address, wrangle_job_title  # Replace 'your_module' with actual module name

class TestWrangleFunctions(unittest.TestCase):
    def test_wrangle_salary(self):
        df = pd.DataFrame({
            'salary': ['10 - 20 triệu', 'tới 15 triệu', 'trên 12 triệu', 'tới 2000 usd', 'thoả thuận']
        })
        df_result = wrangle_salary(df)
        
        expected = pd.DataFrame({
            'negotiable': [False, False, False, False, True],
            'salary_min': [10, np.nan, 12, np.nan, np.nan],
            'salary_max': [20, 15, np.nan, 2000, np.nan],
            'salary_unit': ['M VND', 'M VND', 'M VND', 'USD', 'M VND']
        })
        
        pd.testing.assert_frame_equal(df_result, expected, check_dtype=False)
    
    def test_wrangle_address(self):
        df = pd.DataFrame({
            'address': ['Hà Nội: Ba Đình', 'Hồ Chí Minh: Quận 1', 'Đà Nẵng']
        })
        df_result = wrangle_address(df)
        
        expected = pd.DataFrame({
            'city': ['Hà Nội', 'Hồ Chí Minh', 'Đà Nẵng'],
            'district': ['Ba Đình', 'Quận 1', np.nan]
        })
        
        pd.testing.assert_frame_equal(df_result, expected, check_dtype=False)
    
    def test_wrangle_job_title(self):
        df = pd.DataFrame({
            'job_title': ['Data Scientist', 'Software Engineer', 'Sales Executive', 'HR Manager']
        })
        df_result = wrangle_job_title(df)
        
        expected = pd.DataFrame({
            'job_title': ['Data Scientist', 'Software Engineer', 'Sales Executive', 'HR Manager'],
            'category': ['Data Science', 'IT', 'Sales', 'Human Resources']
        })
        print(df_result)
        print(expected)
        
        pd.testing.assert_frame_equal(df_result, expected, check_dtype=False)

if __name__ == "__main__":
    unittest.main()
