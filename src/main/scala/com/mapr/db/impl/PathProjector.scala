package com.mapr.db.impl

import org.ojai.Document

trait PathProjector {

  def projectPath(path: String): Document
}