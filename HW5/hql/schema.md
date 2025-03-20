### **List of Tables**
| Tables        |
|--------------|
| customers    |
| order_details |
| orders       |
| products     |
| loyalty_program |

---

### **Schema: `customers` Table**
| Column Name | Data Type |
|-------------|----------|
| cust_id     | int      |
| fname       | string   |
| lname       | string   |
| address     | string   |
| city        | string   |
| state       | string   |
| zipcode     | string   |

---

### **Schema: `order_details` Table**
| Column Name | Data Type |
|-------------|----------|
| order_id    | int      |
| prod_id     | int      |

---

### **Schema: `orders` Table**
| Column Name | Data Type |
|-------------|----------|
| order_id    | int      |
| cust_id     | int      |
| order_date  | timestamp |

---

### **Schema: `products` Table**
| Column Name  | Data Type |
|--------------|----------|
| prod_id      | int      |
| brand        | string   |
| name         | string   |
| price        | int      |
| cost         | int      |
| shipping_wt  | int      |

### **Schema: `loyalty_program` Table**
| Column Name | Data Type |
|-------------|----------|
| customer_id | int      |
| first_name  | string   |
| last_name   | string   |
| loyalty_level | string   |
| phone_numbers | map<string,string> |
| order_ids | array\<int> |
| order_summary | struct\<min:double, max:double,avg:double, total:double> |

### **Schema: `ratings` Table**
| Column Name | Data Type |
|-------------|-----------|
| posted      | timestamp |                                            
| cust_id     | int       |
| prod_id     | int       |
| rating      | tinyint   |   
| message     | string    | 
