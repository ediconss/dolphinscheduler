/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { ref, onMounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { useCustomParams, useResources } from '.'
import type { IJsonItem } from '../types'

export function useXuanwuCDC(model: { [field: string]: any }): IJsonItem[] {
  const { t } = useI18n()
  const jsonEditorSpan = ref(0)
  const useResourcesSpan = ref(24)

  const initConstants = () => {
    jsonEditorSpan.value = 24
  }
  onMounted(() => {
    initConstants()
  })

  return [
    {
      type: 'editor',
      field: 'config',
      name: t('project.node.xuanwu_cdc_config'),
      span: jsonEditorSpan,
      validate: {
        trigger: ['input', 'trigger'],
        required: false,
        message: t('project.node.sql_empty_tips')
      }
    },
    ...useCustomParams({ model, field: 'localParams', isSimple: true }),
    useResources(useResourcesSpan)
  ]
}
