package com.dounine.compute.indicators

trait Indicator {

  def des(): String

  def run()

  def offsetName(): String = this.getClass.getName

}
