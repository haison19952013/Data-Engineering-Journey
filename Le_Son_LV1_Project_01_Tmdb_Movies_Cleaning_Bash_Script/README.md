# Linux Command-Line Data Analysis Project

## 📌 Project Overview

This project involves analyzing a movie dataset hosted on a Linux server using command-line tools. The dataset is sourced from TMDb and includes movie metadata such as release dates, revenue, ratings, genres, and cast/crew information.

- **Dataset URL**: [TMDb Movie Dataset](https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv)

The Data Engineering team is tasked with using Linux command-line tools to extract basic statistics and insights from the dataset.

---

## 🔧 Tasks and Requirements

### ✅ Completed Tasks

1. **Sort movies by release date (descending)** and save to a new file  
2. **Filter movies with average rating > 7.5** and save to a new file  
3. **Identify the movies w/ highest and lowest revenue**  
4. **Calculate total revenue of all movies**  
5. **List top 10 most profitable movies**  
6. **Find the most popular director and actor**  
7. **Count number of movies per genre** (e.g., Action, Family, Drama, etc.)

### 💡 Additional Ideas for Analysis

- Analyze profit, ROI, vote average, vote count, and runtime:
  - Grouped by **Director**, **Production Companies**, and **Cast**
  - Trends in **runtime** over time or by genre

---

## 🚀 Approach and Tools

- Utilize Linux command-line skills (e.g., `grep`, `awk`, `sort`, `cut`)
- Explore specialized CSV tools:
  - [`csvkit`](https://csvkit.readthedocs.io/en/latest/)
  - [`csvtool`](https://installati.one/install-csvtool-ubuntu-20-04/)
- Use online regex testing for parsing complex fields:  
  [Regex101](https://regex101.com/)

---

## ✅ Results

All tasks were completed using shell scripting and CLI tools. You can find the project results and scripts in the following repository:

🔗 [GitHub: Data Engineering Journey – Linux Bash Fundamental](https://github.com/haison19952013/Data-Engineering-Journey/tree/master/Linux_Bash_Fundamental)
1. Solution: `1_sort_release_date.sh`
2. Solution: `2_filter_rating.sh`
3. Solution: `3_find_min_max_revenue.sh`
4. Solution: `4_calculate_total_revenue.sh`
5. Solution: `5_find_top10_profit.sh`
6. Solution: `6_find_most_popular_cast.sh` and `6_find_most_popular_director.sh`
7. Solution: `7_find_genre_freq.sh`

---

## 🧱 Challenges & Solutions

| Challenge | Solution |
|----------|----------|
| Parsing `.csv` files with embedded commas | Used `csvtool` and `csvkit` to correctly handle fields |
| Complex regular expressions for filtering text | Utilized [regex101.com](https://regex101.com/) for debugging |
| Data cleaning & formatting | Leveraged both GPT and Claude to troubleshoot and brainstorm |

---

## 📚 Resources

- [Regex101](https://regex101.com/)
- [CSVKit Documentation](https://csvkit.readthedocs.io/en/latest/)
- [CSVTool Installation (Ubuntu 20.04)](https://installati.one/install-csvtool-ubuntu-20-04/)

---

## 🎓 Lessons Learned

- Re-reading documentation for CLI tools was *extremely valuable*
- Tools like `csvtool` and `csvkit` are **essential** for efficient `.csv` processing
- Regular expressions and command-line pipelines are powerful—but tricky—when dealing with real-world data
- Debugging with AI tools like GPT and Claude can speed up problem-solving

## To-Do List
- [ ] Reconduct the exercises without the help of `csvtool` and `csvkit`