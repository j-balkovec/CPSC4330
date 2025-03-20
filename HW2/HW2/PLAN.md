### **1. Directory Structure**

```graphql

project_root/
│── data/           # Stores the 30 input text files
│── output/         # Stores intermediate and final output files
│── math/           # Stores TF-IDF math formulas, logs, and documentation
│── src/            # Driver and job control
│── stubs/          # Contains Mapper, Reducer, Combiner, Partitioner classes
│── lib/            # (Optional) Any additional JARs or dependencies
│── scripts/        # (Optional) Bash scripts for running Hadoop jobs
│── README.md       # Instructions for running the code

```

### **2. MapReduce Job Breakdown**

We can optimize by merging Job 3 and Job 4 as suggested.

### **Job 1: Term Frequency (TF) Calculation**

- **Mapper:**
    - Reads text files.
    - Tokenizes words (case-insensitive, only alphabets).
    - Emits `(word@document, count)`.
- **Combiner (optional):**
    - Performs local aggregation before sending to the reducer.
- **Reducer:**
    - Computes `TF = term count in document / total terms in document`.

### **Job 2: Document Frequency (DF) Calculation**

- **Mapper:**
    - Takes `(word@document, TF)` from Job 1.
    - Emits `(word, document)`.
- **Reducer:**
    - Counts how many documents contain each word (`DF`).
    - Computes `IDF = ln(total documents / DF)`.

### **Job 3: TF-IDF Calculation**

- **Mapper:**
    - Joins `(word@document, TF)` from Job 1 and `(word, IDF)` from Job 2.
- **Reducer:**
    - Computes `TF-IDF = TF * IDF`.
    - Outputs sorted `(word@document, TF-IDF)` pairs.

### **3. Additional Considerations**

- **Partitioner:** To ensure that all records for the same word go to the same reducer.
- **Combiner:** Can be used for efficiency in Job 1.
- **Precision Handling:** Use `DecimalFormat` to keep 8 decimal places.
- **Stopwords:** Not explicitly required, but stopwords will have near-zero TF-IDF values.