package lab2;

public class MySqlDB {
   String sql = "SELECT e.lastName, d.department FROM Employee e, Department d "
           + "WHERE e.deptId = d.deptId ORDER BY e.lastName";
}
