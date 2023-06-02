import java.sql.DriverManager
import java.sql.Connection
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun main() {
    // 데이터베이스 연결 설정, 보안문제로 X로 블라인드 처리
    val url = "jdbc:mariadb://XXX.XX.XXX.XXX:XXXX/game_log"
    val user = "XXXX"
    val password = "XXXXXXXXXXXXXX"

    val connection: Connection = DriverManager.getConnection(url, user, password)

    // 로그 파일이 있는 폴더 경로
    val logFolder = File("XXXX")

    // 처리 완료된 파일이 이동할 폴더 경로
    val processedFolder = File("XXXX")

    // 만약 processed 폴더가 없다면 생성
    if (!processedFolder.exists()) {
        processedFolder.mkdir()
    }

    logFolder.listFiles()?.filter { it.isFile }?.forEach { logFile ->
        processLogFile(connection, logFile)

        // 파일 처리 후, processed 폴더로 이동
        val moveToPath = Paths.get("${processedFolder.absolutePath}/${logFile.name}")
        Files.move(logFile.toPath(), moveToPath, StandardCopyOption.REPLACE_EXISTING)
    }

    connection.close()
}


fun handleLogLineConsume(connection: Connection, tableName: String, fullTimestampStr: String, line: String, countName: String) {
    val parts = line.split("-", limit = 3)
    val itemAcquisitionLog = parts[1].split("(", limit = 2)

    val nickName = itemAcquisitionLog[0].substringBefore("(")
    val userId = itemAcquisitionLog[1].substringAfter("(").substringBefore(")")
    val itemName = parts[2].substringBefore("(")
    val usageInfo = parts[2].split("-")
    var count: Int
    var allCount: Int
    try {
        if(countName == "usecount"){
            count = usageInfo[2].replace("[^0-9]".toRegex(), "").toInt()
        }
        else {
            count = usageInfo[1].replace("[^0-9]".toRegex(), "").toInt()
        }
        allCount = usageInfo.last().substringAfter("-").toIntOrNull() ?: 0
    } catch (e: NumberFormatException) {
        println("Invalid $countName or allCount in line: $line")
        return
    }

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss")
    val timestamp = LocalDateTime.parse(fullTimestampStr, formatter)

    val statement = connection.prepareStatement("INSERT INTO $tableName (timestamp, nickname, userid, itemname, $countName, allcount) VALUES (?, ?, ?, ?, ?, ?)")

    statement.setTimestamp(1, java.sql.Timestamp.valueOf(timestamp))
    statement.setString(2, nickName)
    statement.setString(3, userId)
    statement.setString(4, itemName)
    statement.setInt(5, count)
    statement.setInt(6, allCount)

    statement.executeUpdate()
}

fun handleLogLine(connection: Connection, tableName: String, fullTimestampStr: String, line: String, countName: String) {
    val parts = line.split("-", limit = 3)
    val itemAcquisitionLog = parts[1].split("(", limit = 2)

    val nickName = itemAcquisitionLog[0].substringBefore("(")
    val userId = itemAcquisitionLog[1].substringAfter("(").substringBefore(")")
    val itemName = parts[2].substringBefore("(")
    val usagesInfo = parts[2].split("-")
    var count: Int
    try {
        count = usagesInfo.lastIndex
    } catch (e: NumberFormatException) {
        println("Invalid count in line: $line")
        return
    }

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss")
    val timestamp = LocalDateTime.parse(fullTimestampStr, formatter)

    val statement = connection.prepareStatement("INSERT INTO $tableName (timestamp, nickname, userid, itemname, $countName) VALUES (?, ?, ?, ?, ?)")

    statement.setTimestamp(1, java.sql.Timestamp.valueOf(timestamp))
    statement.setString(2, nickName)
    statement.setString(3, userId)
    statement.setString(4, itemName)
    statement.setInt(5, count)

    statement.executeUpdate()
}

fun handleLogLinePurchase(connection: Connection, tableName: String, fullTimestampStr: String, line: String, countName: String) {
    val parts = line.split("-", limit = 3)
    val itemAcquisitionLog = parts[1].split("(", limit = 2)

    val nickName = itemAcquisitionLog[0].substringBefore("(")
    val userId = itemAcquisitionLog[1].substringAfter("(").substringBefore(")")
    val itemName = parts[2].substringBefore("(")
    val usageInfo = parts[2].split("-")
    var count: Int
    try {

        count = usageInfo[1].replace("[^0-9]".toRegex(), "").toInt()
    } catch (e: NumberFormatException) {
        println("Invalid $countName or allCount in line: $line")
        return
    }

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss")
    val timestamp = LocalDateTime.parse(fullTimestampStr, formatter)

    val statement = connection.prepareStatement("INSERT INTO $tableName (timestamp, nickname, userid, itemname, $countName) VALUES (?, ?, ?, ?, ?)")

    statement.setTimestamp(1, java.sql.Timestamp.valueOf(timestamp))
    statement.setString(2, nickName)
    statement.setString(3, userId)
    statement.setString(4, itemName)
    statement.setInt(5, count)

    statement.executeUpdate()
}

fun processLogFile(connection: Connection, logFile: File) {
    logFile.readLines().forEach { line ->
        println("Read line: $line")

        val timestamp = line.substringBefore(" :")
        val fullTimestampStr = "${logFile.name.substring(0, 10)} ${timestamp}"

        when {
            line.contains("소비아이템획득") -> handleLogLineConsume(connection, "getItemConsume", fullTimestampStr, line,"getcount")
            line.contains("소비아이템사용") -> handleLogLineConsume(connection, "useItem", fullTimestampStr, line, "usecount")
            line.contains("아이템획득") -> handleLogLine(connection, "getItem", fullTimestampStr, line, "getcount")
            line.contains("월드샵아이템구매") -> handleLogLinePurchase(connection, "purchaseItem", fullTimestampStr, line, "purchasecount")
        }
    }
}
