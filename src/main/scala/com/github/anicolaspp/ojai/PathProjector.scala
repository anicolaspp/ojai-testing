package com.github.anicolaspp.ojai

import org.ojai.Document

trait PathProjector {

  def projectPath(path: String): Document
}
