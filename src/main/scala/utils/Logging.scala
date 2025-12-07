package utils

import java.util.logging.{Level, Logger}

/**
 * 전역 로깅 유틸리티 객체입니다.
 * 클래스 이름 정보를 포함하지 않고 공통 로거를 사용합니다.
 * debugMode 변수를 통해 로그 출력 여부를 제어할 수 있습니다.
 */
object Logging {
  private val logger: Logger = Logger.getLogger("DistSortingSystem")

  @volatile var debugMode: Boolean = false

  def isDebugEnabled: Boolean = debugMode

  def logEssential(msg: String): Unit = {
    logger.info(msg)
  }

  def logInfo(msg: String): Unit = {
    if (debugMode) logger.info(msg)
  }

  def logWarning(msg: String): Unit = {
    if (debugMode) logger.warning(msg)
  }

  def logSevere(msg: String): Unit = {
    if (debugMode) logger.severe(msg)
  }
}