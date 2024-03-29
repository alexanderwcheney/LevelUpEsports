<?php include "../inc/dbinfo.inc"; ?>
<html>
<body>
<h1>Esports Database</h1>
<?php

  /* Connect to MySQL and select the database. */
  $connection = mysqli_connect(DB_SERVER, DB_USERNAME, DB_PASSWORD);

  if (mysqli_connect_errno()) echo "Failed to connect to MySQL: " . mysqli_connect_error();

  $database = mysqli_select_db($connection, DB_DATABASE);

  $query = htmlentities($_POST['QUERY']);
?>

<!-- Input form -->
  <table border="0">
  <form action="<?PHP echo $_SERVER['SCRIPT_NAME'] ?>" method="POST">
    <tr>
      <td>MySQL Query</td>
    </tr>
    <tr>
      <td>
        <input type="text" name="QUERY" maxlength="1000" size="100" />
      </td>
      <td>
        <input type="submit" value="Query Database" />
      </td>
    </tr>
    </form>
    <tr>
      <td>
     <form action="<?PHP echo $_SERVER['SCRIPT_NAME'] ?>" method="POST">
        <input type="hidden" name= "QUERY" maxlength="1000" size="100" value="show tables"/>
        <input type="submit" value="Show Tables" />
     </form>
      </td>
    </tr>
  </table>

<!-- Display table data. -->
<table border="1" cellpadding="2" cellspacing="2">

<?php
$start = substr($query, 0 , 6);
if ($start === 'SELECT' || $start === 'select'){
  $result = mysqli_query($connection, $query);
}
$columns = mysqli_fetch_fields($result);
  echo "<tr>";
    foreach ($columns as $column){
      echo "<td>" . $column->name . "</td>";
    }
  echo "</tr>";
while($row = mysqli_fetch_row($result)) {
  echo "<tr>";
    foreach($row as $row_column){
      echo "<td>" . $row_column . "</td>";
    }
  echo "</tr>";
}
?>

</table>

<!-- Clean up. -->
<?php

  mysqli_free_result($result);
  mysqli_close($connection);

?>

</body>
</html>

